/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"html/template"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	json "github.com/goccy/go-json"

	"github.com/mimuret/dtap"
	"github.com/mimuret/tapcat/internal/config"
	"github.com/mimuret/tapcat/internal/format"
	"github.com/mimuret/tapcat/internal/output"
	"github.com/mimuret/tapcat/internal/worker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

type formatAdapter interface {
	Format(dt *dtap.DnstapFlatT) ([]byte, error)
}

var (
	defaultEndTime = time.Unix(1<<63-62135596801, 999999999)
)

type Runtime struct {
	formater  formatAdapter
	startTime time.Time
	endTime   time.Time
	config    *config.Config
}

var runtime = &Runtime{
	endTime: defaultEndTime,
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "tapcat",
	Short: "A query virwer from nats servers",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := setLogLevel(viper.GetViper()); err != nil {
			return err
		}
		if err := prepare(viper.GetViper()); err != nil {
			return err
		}
		return run(viper.GetViper())
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Error(err)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.tapcat.yaml)")
	viper.BindPFlag("config", rootCmd.PersistentFlags().Lookup("config"))
	rootCmd.PersistentFlags().BoolP("dry-run", "d", false, "dry-run mode (default false)")
	viper.BindPFlag("dry-run", rootCmd.PersistentFlags().Lookup("dry-run"))
	rootCmd.PersistentFlags().StringP("loglevel", "l", "info", "log level debug,info,warn,error,fatal (default is info)")
	viper.BindPFlag("loglevel", rootCmd.PersistentFlags().Lookup("loglevel"))
	rootCmd.PersistentFlags().StringP("template", "t", "{{.Type }} {{ .Timestamp }} {{ .Qclass }} {{ .Qtype }} {{ .Qname }}", "for line output go-template")
	viper.BindPFlag("template", rootCmd.PersistentFlags().Lookup("template"))
	rootCmd.PersistentFlags().StringP("output", "o", "line", "line,json,dns (default is line)")
	viper.BindPFlag("output", rootCmd.PersistentFlags().Lookup("output"))
	rootCmd.PersistentFlags().StringP("filename", "f", "", "output path (default is stdout)")
	viper.BindPFlag("filename", rootCmd.PersistentFlags().Lookup("filename"))
	rootCmd.PersistentFlags().StringP("servers", "g", "", "mirror servers comma separated(default is 127.0.0.1:53)")
	viper.BindPFlag("servers", rootCmd.PersistentFlags().Lookup("servers"))
	rootCmd.PersistentFlags().BoolP("dns-rd", "", false, "rd bit for output dns (default is false)")
	viper.BindPFlag("dns-rd", rootCmd.PersistentFlags().Lookup("dns-rd"))
	rootCmd.PersistentFlags().StringP("start", "s", "", "start time RFC3339 format (default is 0)")
	viper.BindPFlag("start", rootCmd.PersistentFlags().Lookup("start"))
	rootCmd.PersistentFlags().StringP("end", "e", "", "end time RFC3339 format (default is Inf)")
	viper.BindPFlag("end", rootCmd.PersistentFlags().Lookup("end"))
	rootCmd.PersistentFlags().String("rotate-exec", "", "exec command with filename when file is rotated")
	viper.BindPFlag("rotate-exec", rootCmd.PersistentFlags().Lookup("rotate-exec"))
	rootCmd.PersistentFlags().Int("queue-size", -1, "rbuf queue size (default 65535)")
	viper.BindPFlag("queue-size", rootCmd.PersistentFlags().Lookup("queue-size"))

	cobra.OnInitialize(initConfig)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetConfigType("toml")
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".tapcat" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".tapcat")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	}
}

func setLogLevel(v *viper.Viper) error {
	switch v.GetString("loglevel") {
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
		return fmt.Errorf("not support loglevel `%s`", v.GetString("loglevel"))
	}
	return nil
}

func prepare(v *viper.Viper) error {
	var err error
	switch viper.GetString("output") {
	case "line":
		tpl, err := template.New("line").Parse(viper.GetString("template"))
		if err != nil {
			return fmt.Errorf("invalid format template `%s`: %w", v.GetString("template"), err)
		}
		runtime.formater = format.NewLineFormater(tpl)
	case "json":
		runtime.formater = format.NewJsonFormater()
	case "dns":
		runtime.formater = format.NewDNSFormater(viper.GetBool("dns-rd"))
	default:
		return fmt.Errorf("not support output `%s`", v.GetString("output"))
	}
	runtime.config, err = config.GetConfig(v)
	if err != nil {
		return fmt.Errorf("failed to get config: %w", err)
	}
	if v.GetInt("queue-size") > 0 {
		runtime.config.QueueSize = v.GetInt("queue-size")
	}

	if v.GetString("start") != "" {
		if runtime.startTime, err = time.Parse(time.RFC3339, v.GetString("start")); err != nil {
			return fmt.Errorf("failed to get start time: %w", err)
		}
	}
	if v.GetString("end") != "" {
		if runtime.endTime, err = time.Parse(time.RFC3339, v.GetString("end")); err != nil {
			return fmt.Errorf("failed to get end time: %w", err)
		}
	}
	if runtime.startTime.After(runtime.endTime) {
		return fmt.Errorf("startTime after endTime")
	}
	return nil
}

func run(v *viper.Viper) error {
	var err error
	sleepDuration := time.Until(runtime.startTime)
	ctx, cancel := context.WithDeadline(context.Background(), runtime.endTime)
	defer cancel()
	if runtime.startTime.Before(time.Now()) {
		log.Infof("start now\n")
	} else {
		log.WithFields(log.Fields{
			"utc": runtime.startTime.UTC().String(),
			"local": runtime.startTime.Local().String(),
		}).Info("start at")
	}
	if runtime.endTime.Equal(defaultEndTime) {
		log.Info("end at infinity\n")
	} else {
		log.WithFields(log.Fields{
			"utc": runtime.endTime.UTC().String(),
			"local": runtime.endTime.Local().String(),
		}).Info("end at")
	}
	var outputer io.WriteCloser
	if viper.GetString("output") == "dns" {
		outputer = output.NewDNS(v.GetString("servers"))
	}else if v.GetString("filename") == "-" {
		outputer = output.NewNothing()
	}else if v.GetString("filename") != "" {
		if outputer, err = output.NewFileOutput(log.StandardLogger(),viper.GetString("filename"), viper.GetString("rotate-exec")); err != nil {
			return fmt.Errorf("failed to create file outpter: %w", err)
		}
		log.WithFields(log.Fields{
			"type": "file",
			"filename": viper.GetString("filename"),
			"rotate-exec": viper.GetString("rotate-exec"),
		}).Info("prepare output")
	} else {
		outputer = output.NewStdout()
		log.WithFields(log.Fields{
			"type": "stdout",
		}).Info("prepare output")
	}
	defer outputer.Close()
	var inCounter = worker.NewCounter()
	var outCounter = worker.NewCounter()
	var lostCounter = worker.NewCounter()
	var lastIn,lastOut,lastLost uint64
	w := worker.NewWorker(runtime.config, inCounter, lostCounter)
	var lastConStats = w.Stats()

	if viper.GetBool("dry-run") {
		return nil
	}
	time.Sleep(sleepDuration)
	log.Info("start now ", time.Now().String())
	if err := w.Run(); err != nil {
		return fmt.Errorf("failed to start worker: %w", err)
	}
	defer w.Stop()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGINT)
	ticker := time.NewTicker(time.Minute)
LOOP:
	for {
		select {
		case <-ticker.C:
			conStats := w.Stats()
			curIn := inCounter.Get()
			curLost := lostCounter.Get()
			curOut := outCounter.Get()
			log.WithFields(log.Fields{
				"queue len": len(w.RBuf.Read()),
				"in queue/min": worker.DiffCount(lastIn, curIn),
				"lost queue/min": worker.DiffCount(lastLost, curLost),
				"out query/min": worker.DiffCount(lastOut, curOut),
				"worker InMsgs": worker.DiffCount(conStats.InMsgs,lastConStats.InMsgs),
				"worker OutMsgs": worker.DiffCount(conStats.OutMsgs,lastConStats.OutMsgs),
				"worker InBytes": worker.DiffCount(conStats.InBytes,lastConStats.InBytes),
				"worker OutBytes": worker.DiffCount(conStats.OutBytes,lastConStats.OutBytes),
				"worker Reconnects": worker.DiffCount(conStats.Reconnects,lastConStats.Reconnects),
			}).Debug("stats")
			lastIn = curIn
			lastLost = curLost
			lastOut = curOut
			lastConStats = conStats
		case <-ctx.Done():
			log.Info("done")
			break LOOP
		case <-sigCh:
			log.Info("signal received")
			break LOOP
		case bs := <-w.RBuf.Read():
			data := []dtap.DnstapFlatT{}
			err := json.Unmarshal(bs, &data)
			if err != nil {
				log.Errorf("can't parse msg: %v", err)
				break
			}
			for _, r := range data {
				outCounter.Inc()
				o, err := runtime.formater.Format(&r)
				if err != nil {
					log.Error("failed to format: %w", err)
					continue
				}
				if _, err := outputer.Write(o); err != nil {
					log.Error("failed to format: %w", err)
					continue
				}
			}
		}
	}
	log.Info("finished ", time.Now().String())
	return nil
}

func init() {
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)
}
