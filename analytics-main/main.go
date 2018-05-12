package main

import	(
	
	"os"
	//"fmt"
	"local/logger"
	"local/symbol"
	
		)



func main() {
	
	// setup the logger
	logger.Init(/*ioutil.Discard*/os.Stdout, os.Stdout, os.Stdout, os.Stderr)
	
	// Start the Symbol service
	symbol.Start()

	// print the supported Markets JSON
	symbol.GetSupportedMarkets()
	
	//
	symbol.GetSymbols("nasdaq","",20)
	symbol.GetSymbols("nasdaq","AB",20)
	
}