package main

import (
	"log"
	"os"

	"gitlab.com/ethanlewis787/fun-with-raft/internal/storage"

	"github.com/coreos/etcd/raft/raftpb"

	"github.com/urfave/cli"
	"go.uber.org/zap"
)

type appConfig struct {
	id      int
	port    string
	join    bool
	cluster string
}

func main() {
	config := &appConfig{}
	// Init the urfave cli app
	app := cli.NewApp()
	app.Name = "fwr"
	app.Usage = "Fun-with-raft ... playing around with the raft consensus alg."
	app.Version = "v0.0.0" // major,minor,patch
	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:        "id",
			Value:       1,
			Usage:       "Id of the node",
			EnvVar:      "NODE_ID",
			Destination: &config.id,
		},
		cli.StringFlag{
			Name:        "port",
			Value:       "9121",
			Usage:       "port of the rest api",
			EnvVar:      "PORT",
			Destination: &config.port,
		},
		cli.BoolFlag{
			Name:        "join",
			Usage:       "Joining a cluster or not",
			EnvVar:      "JOIN",
			Destination: &config.join,
		},
		cli.StringFlag{
			Name:        "cluster",
			Value:       "http://127.0.0.1:9021",
			Usage:       "Comma seperated cluster list 127.0.0.1:1287,127.0.0.1:1288",
			EnvVar:      "CLUSTER",
			Destination: &config.cluster,
		},
	}
	// ------- Main Application function -------
	app.Action = func(cliCTX *cli.Context) error {
		// Init zap logger
		zlogger, err := zap.NewDevelopment()
		if err != nil {
			return err
		}
		zlogger.Info("fun with raft!")

		propose := make(chan string)
		defer close(propose)
		configChanges := make(chan raftpb.ConfChange)
		defer close(configChanges)

		var fsm *storage.FiniteStateMachine
		commits, errors, snapShotterReady := raftconsensus.NewRaftNode()
		
		return nil
	}
	// Start main
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
