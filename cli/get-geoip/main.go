package main

import (
	"io"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"
)

func main() {
	response, err := http.Get("https://github.com/Dreamacro/maxmind-geoip/releases/latest/download/Country.mmdb")
	if err != nil {
		logrus.Fatal(err)
	}
	defer response.Body.Close()
	output, err := os.Create("Country.mmdb")
	if err != nil {
		logrus.Fatal(err)
	}
	defer output.Close()
	_, err = io.Copy(output, response.Body)
	if err != nil {
		os.RemoveAll("Country.mmdb")
		logrus.Fatal(err)
	}
}