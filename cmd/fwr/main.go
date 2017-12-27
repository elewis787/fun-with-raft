package main

import (
	"log"
	"os"

	"github.com/urfave/cli"
	"go.uber.org/zap"
)

func main() {
	// Init the urfave cli app
	app := cli.NewApp()
	app.Name = "fwr"
	app.Usage = "Fun-with-raft ... playing around with the raft consensus alg."
	app.Version = "v0.0.0" // major,minor,patch
	app.Flags = []cli.Flag{}
	app.Commands = []cli.Command{}
	// ------- Main Application function -------
	app.Action = func(cliCTX *cli.Context) error {
		// Init zap logger
		zlogger, err := zap.NewDevelopment()
		if err != nil {
			return err
		}
		zlogger.Info("fun")
		return nil
	}
	// Start main
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
