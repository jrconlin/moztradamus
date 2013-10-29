/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package storage

// thin memcache wrapper

/** This library uses libmemcache (via gomc) to do most of it's work.
 * libmemcache handles key sharding, multiple nodes, node balancing, etc.
 */

import (
	"github.com/ianoshen/gomc"

	"mozilla.org/util"

	"bufio"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	config        util.JsMap
	no_whitespace *strings.Replacer = strings.NewReplacer(" ", "",
		"\x08", "",
		"\x09", "",
		"\x0a", "",
		"\x0b", "",
		"\x0c", "",
		"\x0d", "")
)

type Storage struct {
	config     util.JsMap
	mcs        chan gomc.Client
	logger     *util.HekaLogger
	mc_timeout time.Duration
	servers    []string
}

var mcsPoolSize int32

// Attempting to use dynamic pools seems to cause all sorts of problems.
// The first being that the pool channel can't be resized once it's built
// in New(). The other being that during shutdown, go throws a popdefer
// phase error that I've not been able to chase the source. Since this doesn't
// happen for statically set pools, I'm going to go with that for now.
// var mcsMaxPoolSize int32

// ChannelRecord
// I am using very short IDs here because these are also stored as part of the GLOB
// and space matters in MC.
type record struct {
	L int64  // Last touched
}

type StorageError struct {
	err string
}

func (e StorageError) Error() string {
	// foo call so that import log doesn't complain
	_ = log.Flags()
	return "StorageError: " + e.err
}

// Returns a new slice with the string at position pos removed or
// an equivilant slice if the pos is not in the bounds of the slice
func remove(list [][]byte, pos int) (res [][]byte) {
	if pos < 0 || pos == len(list) {
		return list
	}
	return append(list[:pos], list[pos+1:]...)
}

// Use the AWS system to query for the endpoints to use.
// (Allows for dynamic endpoint assignments)
func getElastiCacheEndpoints(configEndpoint string) (string, error) {
	c, err := net.Dial("tcp", configEndpoint)
	if err != nil {
		return "", err
	}
	defer c.Close()

	reader, writer := bufio.NewReader(c), bufio.NewWriter(c)
	writer.Write([]byte("config get cluster\r\n"))
	writer.Flush()

	reader.ReadString('\n')
	reader.ReadString('\n')
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", nil
	}

	endPoints := strings.Split(line, " ")
	if len(endPoints) < 1 {
		return "", errors.New("Elasticache returned no endPoints")
	}

	retEndpoints := make([]string, 0)
	for _, v := range endPoints {
		endPoint := strings.Split(v, "|")
		if len(endPoint) < 3 {
			continue
		}
		retEndpoints = append(retEndpoints, fmt.Sprintf("%s:%s", endPoint[1], strings.TrimSpace(endPoint[2])))
	}
	return strings.Join(retEndpoints, ","), nil
}

func getElastiCacheEndpointsTimeout(configEndpoint string, seconds int) (string, error) {
	type strErr struct {
		ep  string
		err error
	}

	ch := make(chan strErr)

	go func() {
		ep, err := getElastiCacheEndpoints(configEndpoint)
		ch <- strErr{ep, err}
	}()
	select {
	case se := <-ch:
		return se.ep, se.err
	case <-time.After(time.Duration(seconds) * time.Second):
		return "", errors.New("Elasticache config timeout")
	}

}

// Convert a user readable UUID string into it's binary equivalent
func cleanID(id string) []byte {
	res, err := hex.DecodeString(strings.TrimSpace(strings.Replace(id, "-", "", -1)))
	if err == nil {
		return res
	}
	return nil

}

// break apart a binary Primary Key
func binResolvePK(pk []byte) (uaid, chid []byte, err error) {
	return pk[:15], pk[16:], nil
}

// Encode a key for Memcache to use.
func keycode(key []byte) string {
	// Sadly, can't use full byte chars for key values, so have to encode
	// to base64. Ideally, this would just be
	// return string(key)
	return base64.StdEncoding.EncodeToString(key)
}
func keydecode(key string) []byte {
	ret, _ := base64.StdEncoding.DecodeString(key)
	return ret
}

func New(opts util.JsMap, logger *util.HekaLogger) *Storage {
	config = opts
	var ok bool
	var err error

	if configEndpoint, ok := config["elasticache.config_endpoint"]; ok {
		memcacheEndpoint, err := getElastiCacheEndpointsTimeout(configEndpoint.(string), 2)
		if err == nil {
			config["memcache.server"] = memcacheEndpoint
		} else {
			fmt.Println(err)
			if logger != nil {
				logger.Error("storage", "Elastisearch error.",
					util.Fields{"error": err.Error()})
			}
		}
	}

	if _, ok = config["memcache.server"]; !ok {
		config["memcache.server"] = "127.0.0.1:11211"
	}

	timeout, err := time.ParseDuration(util.MzGet(config, "db.handle_timeout", "5s"))
	if err != nil {
		if logger != nil {
			logger.Error("storage", "Could not parse db.handle_timeout",
				util.Fields{"err": err.Error()})
		}
		timeout = 10 * time.Second
	}
	if _, ok = config["memcache.pool_size"]; !ok {
		config["memcache.pool_size"] = "100"
	}
	if config["memcache.pool_size"], err =
		strconv.ParseInt(config["memcache.pool_size"].(string), 0, 0); err != nil {
		config["memcache.pool_size"] = 100
	}
	poolSize := int(config["memcache.pool_size"].(int64))
	if _, ok = config["db.timeout_live"]; !ok {
		config["db.timeout_live"] = "259200"
	}

	if _, ok = config["db.timeout_reg"]; !ok {
		config["db.timeout_reg"] = "10800"
	}

	if _, ok = config["db.timeout_del"]; !ok {
		config["db.timeout_del"] = "86400"
	}
	if _, ok = config["shard.default_host"]; !ok {
		config["shard.default_host"] = "localhost"
	}
	if _, ok = config["shard.current_host"]; !ok {
		config["shard.current_host"] = config["shard.default_host"]
	}
	if _, ok = config["shard.prefix"]; !ok {
		config["shard.prefix"] = "_h-"
	}
	/*
		i, err := strconv.ParseInt(util.MzGet(config,
			"memcache.max_pool_size", "400"), 0, 10)
		if err != nil {
			if logger != nil {
				logger.Error("storage", "Could not parse memcache.max_pool_size",
					util.Fields{"error": err.Error()})
			}
			mcsMaxPoolSize = 400
		}
		mcsMaxPoolSize = int32(i)
	*/

	if logger != nil {
		logger.Info("storage", "Creating new gomc handler", nil)
	}
	// do NOT include any spaces
	servers := strings.Split(
		no_whitespace.Replace(config["memcache.server"].(string)),
		",")

	mcs := make(chan gomc.Client, poolSize)
	for i := 0; i < poolSize; i++ {
		mcs <- newMC(servers, config, logger)
	}

	return &Storage{
		mcs:        mcs,
		config:     config,
		logger:     logger,
		mc_timeout: timeout,
		servers:    servers,
	}
}

// Generate a new Memcache Client
func newMC(servers []string, config util.JsMap, logger *util.HekaLogger) (mc gomc.Client) {
	var err error
	/*	if mcsPoolSize >= mcsMaxPoolSize {
			return nil
		}
	*/
	mc, err = gomc.NewClient(servers, 1, gomc.ENCODING_GOB)
	if err != nil {
		logger.Critical("storage", "CRITICAL HIT!",
			util.Fields{"error": err.Error()})
	}
	// internally hash key using MD5 (for key distribution)
	mc.SetBehavior(gomc.BEHAVIOR_KETAMA_HASH, 1)
	// Use the binary protocol, which allows us faster data xfer
	// and better data storage (can use full UTF-8 char space)
	mc.SetBehavior(gomc.BEHAVIOR_BINARY_PROTOCOL, 1)
	//mc.SetBehavior(gomc.BEHAVIOR_NO_BLOCK, 1)
	// NOTE! do NOT set BEHAVIOR_NOREPLY + Binary. This will cause
	// libmemcache to drop into an infinite loop.
	if v, ok := config["memcache.recv_timeout"]; ok {
		d, err := time.ParseDuration(v.(string))
		if err == nil {
			mc.SetBehavior(gomc.BEHAVIOR_SND_TIMEOUT,
				uint64(d.Nanoseconds()*1000))
		}
	}
	if v, ok := config["memcache.send_timeout"]; ok {
		d, err := time.ParseDuration(v.(string))
		if err == nil {
			mc.SetBehavior(gomc.BEHAVIOR_RCV_TIMEOUT,
				uint64(d.Nanoseconds()*1000))
		}
	}
	if v, ok := config["memcache.poll_timeout"]; ok {
		d, err := time.ParseDuration(v.(string))
		if err == nil {
			mc.SetBehavior(gomc.BEHAVIOR_POLL_TIMEOUT,
				uint64(d.Nanoseconds()*1000))
		}
	}
	if v, ok := config["memcache.retry_timeout"]; ok {
		d, err := time.ParseDuration(v.(string))
		if err == nil {
			mc.SetBehavior(gomc.BEHAVIOR_RETRY_TIMEOUT,
				uint64(d.Nanoseconds()*1000))
		}
	}
	atomic.AddInt32(&mcsPoolSize, 1)
	return mc
}

func (self *Storage) fetchRec(pk []byte) (result *record, err error) {
	//fetch a record from Memcache
	result = &record{}
	if pk == nil {
		err = StorageError{"Invalid Primary Key"}
		return nil, err
	}

	defer func() {
		if err := recover(); err != nil {
			if self.logger != nil {
				self.logger.Error("storage",
					fmt.Sprintf("could not fetch record for %s", pk),
					util.Fields{"primarykey": keycode(pk),
						"error": err.(error).Error()})
			}
		}
	}()

	mc, err := self.getMC()
	defer self.returnMC(mc)
	if err != nil {
		return nil, err
	}
	//mc.Timeout = time.Second * 10
	err = mc.Get(keycode(pk), result)
	if err != nil && strings.Contains("NOT FOUND", err.Error()) {
		err = nil
	}
	if err != nil {
		if self.logger != nil {
			self.logger.Error("storage",
				"Get Failed",
				util.Fields{"primarykey": hex.EncodeToString(pk),
					"error": err.Error()})
		}
		return nil, err
	}

	if self.logger != nil {
		self.logger.Debug("storage",
			"Fetched",
			util.Fields{"primarykey": hex.EncodeToString(pk),
				"result": fmt.Sprintf("last: %d",
					result.L),
			})
	}
	return result, err
}

func (self *Storage) storeRec(pk []byte, rec *record) (err error) {
	if pk == nil {
		return StorageError{"Invalid Primary Key"}
	}

	if rec == nil {
		err =  StorageError{"No Data to Store"}
		return err
	}

	rec.L = time.Now().UTC().Unix()

	mc, err := self.getMC()
	defer self.returnMC(mc)
	if err != nil {
		return err
	}

	err = mc.Set(keycode(pk), rec, 15*time.Minute)
	if err != nil {
		if self.logger != nil {
			self.logger.Error("storage",
				"Failure to set item",
				util.Fields{"primarykey": keycode(pk),
					"error": err.Error()})
		}
	}
	return err
}

func (self *Storage) RegPing(pk []byte) (err error) {
    rec := record{L: time.Now().Unix()}
    log.Printf("Storing rec %q", rec)
    return self.storeRec(pk, &rec)
}

func (self *Storage) CheckPing(pk []byte) (rep int64, err error) {
    log.Printf("Looking for rec %s", pk)
    if rec, err := self.fetchRec(pk); err == nil {
        log.Printf("Found rec %q", rec)
        return rec.L, nil
    } else {
        log.Printf("Nope")
        return 0, err
    }
}

func (self *Storage) Close() {
	//	self.mc.Close()
}
func (self *Storage) Status() (success bool, err error) {
	defer func() {
		if recv := recover(); recv != nil {
			success = false
			err = recv.(error)
			return
		}
	}()

	fake_id := "_fake_id"
	key := "status_" + fake_id
	mc, err := self.getMC()
	defer self.returnMC(mc)
	if err != nil {
		return false, err
	}
	err = mc.Set("status_"+fake_id, "test", 6*time.Second)
	if err != nil {
		return false, err
	}
	var val string
	err = mc.Get(key, &val)
	if err != nil || val != "test" {
		return false, errors.New("Invalid value returned")
	}
	mc.Delete(key, time.Duration(0))
	return true, nil
}

func (self *Storage) returnMC(mc gomc.Client) {
	if mc != nil {
		atomic.AddInt32(&mcsPoolSize, 1)
		self.mcs <- mc
	}
}

func (self *Storage) getMC() (gomc.Client, error) {
	select {
	case mc := <-self.mcs:
		if mc == nil {
			log.Printf("NULL MC!")
			return nil, StorageError{"Connection Pool Saturated"}
		}
		atomic.AddInt32(&mcsPoolSize, -1)
		return mc, nil
	case <-time.After(self.mc_timeout):
		// As noted before, dynamic pool sizing turns out to be deeply
		// problematic at this time.
		// TODO: Investigate why go throws the popdefer phase error on
		// socket shutdown.
		/*		if mcsPoolSize < mcsMaxPoolSize {
					if self.logger != nil {
						self.logger.Warn("storage", "Increasing Pool Size",
							util.Fields{"poolSize": strconv.FormatInt(int64(mcsPoolSize+1), 10)})
					}
					return newMC(self.servers,
						self.config,
						self.logger), nil
				} else {
		*/
		if self.logger != nil {
			self.logger.Error("storage", "Connection Pool Saturated!", nil)
		} else {
			log.Printf("Connection Pool Saturated!")
		}
		// mc := self.NewMC(self.servers, self.config)
		return nil, StorageError{"Connection Pool Saturated"}
		//		}
	}
}

// o4fs
// vim: set tabstab=4 softtabstop=4 shiftwidth=4 noexpandtab
