package main

import (
    "mozilla.org/util"
    "mozilla.org/moztradamus"

    "flag"
    "fmt"
    "net/http"
    "log"
    "os"
    "os/signal"
    "runtime"
    "syscall"
    "strings"
)

var (
    configFile *string = flag.String("config", "config.ini", "Configuration File")
    profile *string = flag.String("profile", "", "Profile file output")
    memProfile *string = flag.String("memProfile", "", "Profile file output")
    logging     * int = flag.Int("logging", 10, "Logging level (0=none...10=verbose")
    //logger  *util.HekaLogger
    //store   *storage.Storage
)


const (
    VERSION = "0.1"
    SIGUSR1 = syscall.SIGUSR1
)


func logger(level int, msg string) {
    if level < *logging {
        log.Printf(msg);
    }
}


func main() {
    flag.Parse()

    // Configuration
    config := util.MzGetConfig(*configFile)
    config["VERSION"]=VERSION
    runtime.GOMAXPROCS(runtime.NumCPU())
    handlers := moztradamus.NewHandler(config)

    // Signal handler
    sigChan := make(chan os.Signal)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGHUP, SIGUSR1)

    // Rest Config
    errChan := make(chan error)
    host := util.MzGet(config, "host", "localhost")
    port := util.MzGet(config, "port", "8080")
    var RESTMux *http.ServeMux = http.DefaultServeMux
    var verRoot = strings.SplitN(VERSION, ".", 2)[0]
    RESTMux.HandleFunc(fmt.Sprintf("/%s/ping/", verRoot), handlers.PingHandler)
    RESTMux.HandleFunc("/status/", handlers.StatusHandler)

    logger(5,"startup...")
    go func() {
        errChan <- http.ListenAndServe(host + ":" + port, nil)
    }()

    select {
    case err := <-errChan:
        if err != nil {
            panic ("ListenAndServe: " + err.Error())
        }
    case <-sigChan:
        logger(5, "Shutting down...")
    }

}
