package auth

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

// TODO: [doug] either change create an AuthProvider interface that this implements or use a client-side interceptor
type client struct {
	serviceURL string
	httpClient *http.Client
}

type AuthResp struct {
	AuthToken string `json:"authToken"`
}

type AuthReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func GetToken() string {
	username := "cassandra"
	password := "cassandra"

	AuthReq := AuthReq{
		Username: username,
		Password: password,
	}
	jsonString, err := json.Marshal(AuthReq)
	if err != nil {
		log.Fatalf("error marshalling request: %v", err)
	}

	client := getClient()

	req, err := http.NewRequest(http.MethodPost, "http://localhost:8081/v1/auth", bytes.NewBuffer(jsonString))
	if err != nil {
		log.Fatalf("error creating request: %v", err)
	}

	req.Header.Add("Content-Type", "application/json")
	response, err := client.httpClient.Do(req)
	if err != nil {
		log.Fatalf("error calling partnerservicecontrol API: %v", err)
	}

	defer func() {
		err := response.Body.Close()
		if err != nil {
			log.Warnf("unable to close response body: %v", err)
		}
	}()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalf("error reading response body: %v", err)
	}

	ar := AuthResp{}
	err = json.Unmarshal(body, &ar)
	if err != nil {
		log.Fatalf("error unmarshalling response body: %v", err)
	}

	return ar.AuthToken
}

func getClient() *client {
	return &client{
		serviceURL: "",
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}
