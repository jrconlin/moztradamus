package moztradamus

import(
    "mozilla.org/util"
    "mozilla.org/moztradamus/storage"

    "crypto/rand"
    "encoding/base64"
    //"encoding/json"
    "net/http"
    // "net/url"
    "fmt"
    "math"
    "strings"
    "log"
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

func (self *Handler) newToken() (string, error) {

    token := make([]byte, 16)
    n, err := rand.Read(token)
    if n != len(token) || err != nil {
        return "", err
    }
    return base64.StdEncoding.EncodeToString(token), nil
}


func (self *Handler) PingHandler(resp http.ResponseWriter, req *http.Request) {
    var response []byte
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

    // token := elements[len(elements)-1]
    response = []byte(token)

    resp.Write(response)
}

func (self *Handler) StatusHandler(resp http.ResponseWriter, req *http.Request) {
    OK := "OK"
    reply := fmt.Sprintf("{\"status\":\"%s\", \"version\":\"%s\"}\n",
        OK, self.config["VERSION"])
    resp.Write([]byte(reply))
}

