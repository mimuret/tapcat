/*
 * Copyright (c) 2018 Manabu Sonoda
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"text/template"

	"github.com/mimuret/dtap"

	_ "net/http/pprof"
	"runtime/debug"

	_ "github.com/mailru/go-clickhouse"

	log "github.com/sirupsen/logrus"
)

var msgCh chan *dtap.DnstapFlatT

var DefaultTempalte = "{{.Type }} {{ .Timestamp }} {{ .Qclass }} {{ .Qtype }} {{ .Qname }}"
var tpl *template.Template

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

var (
	flagConfigFile = flag.String("c", "config.toml", "config file path")
	flagLogLevel   = flag.String("d", "info", "log level(debug,info,warn,error,fatal)")
	flagTemplate   = flag.String("t", "", "template")
	flagOutType    = flag.String("f", "line", "output format (line,json)")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]...\n", os.Args[0])
	flag.PrintDefaults()
}

func makeTemplate(path string) {
	var err error
	var templateStr = DefaultTempalte
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			log.Fatal("can't open template file: %v", err)
		}
		defer f.Close()
		bs, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatal("can't read template file: %v", err)
		}
		templateStr = string(bs)
	}
	tpl, err = template.New("line").Parse(templateStr)
	if err != nil {
		log.Fatal("can't create template: %v", err)
	}
}

func output(viewFunc func(*dtap.DnstapFlatT), w *Worker) {
	for {
		select {
		case bs := <-w.rBuf.Read():
			data := []dtap.DnstapFlatT{}
			err := json.Unmarshal(bs, &data)
			if err != nil {
				log.Errorf("can't parse msg: %v", err)
				break
			}
			for _, r := range data {
				viewFunc(&r)
			}
		}
	}
}

func lineOutput(dt *dtap.DnstapFlatT) {
	b := bytes.NewBuffer(nil)
	err := tpl.Execute(b, dt)
	if err != nil {
		log.Error("can't exec template: %v", err)
	}
	log.Info(b.String())
}

func jsonOutput(dt *dtap.DnstapFlatT) {
	bs, err := json.Marshal(dt)
	if err != nil {
		log.Error("can't make jsonstr")
	}
	fmt.Println(string(bs))
}

func main() {
	var err error
	debug.FreeOSMemory()

	flag.Usage = usage
	flag.Parse()
	// set log level
	switch *flagLogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	default:
		usage()
		os.Exit(1)
	}
	c, err := NewConfigFromFile(*flagConfigFile)
	if err != nil {
		log.Fatal(err)
	}
	worker := NewWorker(c)
	go worker.Run()
	switch *flagOutType {
	case "line":
		makeTemplate(*flagTemplate)
		go output(lineOutput, worker)
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
		go output(jsonOutput, worker)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGABRT)
	<-sigCh
	worker.Stop()
}
