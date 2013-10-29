package moztradamus

import(
    "mozilla.org/util"
    "mozilla.org/moztradamus/storage"

    "crypto/rand"
    "encoding/base64"
    "encoding/json"
    "net/http"
    // "net/url"
    "io"
    "fmt"
    "math"
    "strings"
    "log"
    "time"
)

type Handler struct {
    config util.JsMap
    logger *util.HekaLogger
    store  *storage.Storage
}

func NewHandler(config util.JsMap, store *storage.Storage, logger *util.HekaLogger) *Handler {
    return &Handler{config: config,
        store: store,
        logger: logger}
}

func (self *Handler) err(resp http.ResponseWriter, msg string, status int) {
    if status == 0 {
        status = 500
    }
    http.Error(resp, msg, status)
    return
}


func (self *Handler) newToken() (string, error) {

    token := make([]byte, 16)
    n, err := rand.Read(token)
    if n != len(token) || err != nil {
        return "", err
    }
    return base64.StdEncoding.EncodeToString(token), nil
}

func (self *Handler) PingHandler(resp http.ResponseWriter, req *http.Request) {
    var token string

    elements := strings.Split(req.URL.Path,"/")
    log.Printf("elements: %v, %d, %s", elements, len(elements), elements[3])
    if len(elements[3]) == 0 {
        token, _ = self.newToken()
        log.Printf("New token: %s", token)
    } else {
        maxLen := int(math.Min(float64(16), float64(len(elements[3]))))
        log.Printf("maxLen %d", maxLen)
        token = elements[3][0:maxLen]
        log.Printf("maxLen %s", token)
    }

    err := self.store.RegPing([]byte(token))
    if err != nil {
        http.Error(resp,
            fmt.Sprintf("Could not register token %s", token),
            500)
        return
    }
    // token := elements[len(elements)-1]

    resp.Write([]byte(token+"\n"))
}


func (self *Handler) PollHandler(resp http.ResponseWriter, req *http.Request) {
    var result map[string]int

    result = make(map[string]int)

    // get the list of ids to poll
    if req.Method != "POST" {
        self.err(resp, "", http.StatusMethodNotAllowed)
        return
    }
    // read up to 10MB of data:
    body := make([]byte, 10485760)
    blen, _ := io.ReadFull(req.Body, body)
    body = body[:blen]
    log.Printf("Body: %q", body)
    for _ , item := range strings.Split(string(body), ",") {
        item := strings.TrimSpace(item)
        self.logger.Info("poll", item, nil)
        lastPing, err := self.store.CheckPing([]byte(item))
        if err != nil {
            self.logger.Error("poll",
                fmt.Sprintf("Item not found %s", item),
                nil)
            delete (result, item)
            continue
        } else {
            result[item]=int(time.Now().UTC().Unix() - lastPing)
        }
    }

    reply,_ := json.Marshal(result)
    resp.Write(reply)
    resp.Write([]byte("\n"))
}

func (self *Handler) StatusHandler(resp http.ResponseWriter, req *http.Request) {
    OK := "OK"
    reply := fmt.Sprintf("{\"status\":\"%s\", \"version\":\"%s\"}\n",
        OK, self.config["VERSION"])
    resp.Write([]byte(reply))
}

