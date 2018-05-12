package symbol


import (
	"encoding/json"
	
	"github.com/jlaffaye/ftp"
	"local/logger"
	"fmt"
	
	
)



/*
This object deals with normalisation of data from different markets abstracting the analytic functions away from
the different implementations of individual markets.

Input data can come from a REST endpoint, and ftp download, or a local file. 

Services provided:
- Maintianing a per market list of ticker symbols
- listing the available options
- getting current share price
- geting current option price
- getting historical share data 


API:

ToDo: Have a think about the APIs and document here. Probably need, by market, search, read all, read single and list APIs

*/
type security struct {
	symbol       string
	securityName string
	options      []option
}

type option struct {
	desc        string // Put Call etc
	term        int    // 30,60,90 days etc
}

type markets = map[string][]security

var marketData markets


/*****

	PUBLIC FUNCTIONS
	
*****/


/* TODO:

getting price data for stocks 

Try https://www.alphavantage.co/

This renders the open, close, high, low, and volume is returned for each date.

Example call:

https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&apikey=demo


Getting Option pricing data

http://www.strategic-options.com/insight/how-to-get-option-prices-for-free-api-yahoo/


url: https://query1.finance.yahoo.com/v7/finance/options/AAPL

Obviously, AAPL is apple computers. This will return nicely formed JSON data. It will also return all the existing expiration dates at the top of the json object.

If you want a specific date the you just add the ?date=1505433600 to the end of the URL.  The date is converted into unix time, which you can translate here.

https://query1.finance.yahoo.com/v7/finance/options/CMG?date=1505433600




*/



// Setup the symbol micro service and associated APIs										 
func Start () error{
	
	marketData = make (markets) // setup the main data table 
	
	// Retrieve the market data to build the table. It will be different for each data source
	err:=addNasdaqSymbols()
	if err != nil {
		return err
	}else {
		return nil
	}	
}


type MarketList struct {
	Markets []string
}

// Returns a JSON list of the names of markets that we have symbol data for
func GetSupportedMarkets ()  {
	
var supportedMarkets MarketList

	for k:= range marketData{
		supportedMarkets.Markets= append(supportedMarkets.Markets,k)
		logger.Trace.Printf("market:%s",k)
		
	}
	data,err:=json.Marshal(supportedMarkets)
	if err !=nil{
		logger.Error.Println("JSON Marshalling failed")
	}
	logger.Trace.Printf("SupportedMarkets:\n%s",data)
}


type SymbolList struct {
	Symbols []string
}
/* 	inputs:
		market: 	eg nasdaq
		lastSymbol: the last symbol received, "" == start from the begining
		length:		the number of symbols to be returned. max 100.
	
	ouput:
		JSON array of symbols traded on a market of 'length' elements
*/	

func GetSymbols (market string, lastSymbol string, length int)  {
	var symbols SymbolList
	var lastSymbolIndex int
	endOfList:= false
	
	if length > 100{
		length = 100
	}
	
	fmt.Printf("lastSymbol : %s\nlength : %d\n",lastSymbol,length)
	
	// search for last symbol to pick up where the last call left off
	if lastSymbol == ""{
		lastSymbolIndex=0			// we are starting from the begining
	}else {
		var i int
		for i=0;marketData[market][i].symbol != lastSymbol;i++ {
			continue				// search through the array of market symbols until we find the last one that was sent.
		}
		lastSymbolIndex = i+1 
		
		// check bounds and if we are at the end of the list of symbols
		if lastSymbolIndex + length > len(marketData[market]){
			length = len(marketData[market]) - lastSymbolIndex
			endOfList = true
			if length <0{
				length=0
				logger.Warning.Printf("lastSymbolIndex out of bounds")
			}
			
		}
	}
	
	fmt.Printf("lastSymbolIndex=%d\n",lastSymbolIndex)
	
	// extract the symbols from the data store
	for i:=lastSymbolIndex;i<(lastSymbolIndex+length);i++ {
		symbols.Symbols = append(symbols.Symbols,marketData[market][i].symbol)
	}
	if endOfList == true{
		symbols.Symbols = append(symbols.Symbols,"")	// append an empty string to mark the end of the symbol list
	}
	// convert to json
	data,err:=json.Marshal(symbols)
	if err !=nil{
		logger.Error.Println("JSON Marshalling failed")
	}
	
	logger.Trace.Printf("Symbol Data:\n%s",data)
}


/*****

	PRIVATE FUNCTIONS
	
*****/


/* retrives data from a nasdaq ftp site and stores it in the symbol table under the key "nasdaq" */
func addNasdaqSymbols () error {

	// details from: http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs  
	// we are interested in this file: nasdaqtraded.txt which is approx 650K in size
	// NB nasdaqlisted.txt has a different column format to nasdaqtraded.txt.
	
	response,err:=getftpData ("ftp.nasdaqtrader.com:21","nasdaqtraded.txt","symboldirectory","anonymous","anonymous" )
	if err != nil {
		logger.Warning.Println (" Couldnt read the Nasdaq ftp site: ftp.nasdaqtrader.com")
		return err
	}
	
	var mem [700 * 1024]byte		//file we are interested in is about 650K bytes
	buf := mem[0:]
	
	n,err:=response.Read (buf)
	for n>0{
	
		if err != nil{
			logger.Error.Println("Error reading ftp response buffer. Error:%s", err)
		}
		
		// iterate through the response buffer to extract data, normalise and store.
		// Nasdaq data columns are separated by the '|' character. There are 12 columns and we are interested in cols 1 (Symbol) & 2 (security name) 
		
		row:=0
		col:=0
		start1:=-1	//start index of column 1
		start2:= -1	// "	  "		 column 2
		var symbol string	//Ticker symbol
		var name string		// Company name and details
		
		for i:=0;i<n;i++{
			
			if buf[i] == 0x0A{		// 0x0A == New Line
				row++
				continue
			}
			if row >0{		// ignore the first row as it has the column headings in it
				if string(buf[i]) == "|" { // columns delimited by | symbols
					col++
					continue		// go to next iteration of for loop
				}
				
				switch  col {
				
				case 1:
					if start1 == -1{
						start1 = i		// mark the index of the start of the data in the column
						logger.Trace.Printf("row:%d coumn:%d index:%d letter:%d",row,col,i,buf[i])
					}else{
						continue		// keep going until the end of the column 1
					}
				
				case 2:					// this is the end of col 1 and the start of col 2
					if start2 == -1{
					symbol= string (buf[start1:i-1])	// extract the data from column 1
					
					logger.Trace.Printf("column:%d row:%d index:%d symbol:%s",col,row,i,symbol)
					
					start2 = i			// mark the index of column 2 start
					}else {
						continue		// keep going until the end of the column 2
					}
					
				case 3:					// end of column 2, now index is at the start of column 3
					
					if start1 != -1 && start2 !=-1{ 	//only execute this block once, right after extracting the data from column 2
						name = string (buf[start2:i-1])
						
						logger.Trace.Printf("column:%d row:%d index:%d name:%s ",col,row,i,name)
						
						// write the data we have collected to the map.
						//ToDo Add a mutex around this to make it thread safe.
						marketData["nasdaq"] = append (marketData["nasdaq"],security{symbol,name,nil})
										
						start1 = -1		// reset the indexes.
						start2 = -1
					}else {
						continue		// keep going until the end of column 3	
					}	
				
				case 11:
					col = 0 				// there are 12 columns per row.
					
					
				default:
					continue 			// iterate through the current column until there is something to do.
				
				}
			}
		}
		n,err =response.Read (buf)	// read the next chunk of data.	
	}
	

	
	// close the ftp connection
	response.Close()
	return err
	
} 
	


func getftpData (server string, fileName string, path string, uname string, passwd string) (*ftp.Response, error) {
	
	client, err := ftp.Dial(server)
	if err != nil {
	  logger.Trace.Println("ftp connect error:", server,err)
	  return nil,err
	}
	if err := client.Login(uname, passwd); err != nil {
	  logger.Trace.Println ("ftp login error: ", server,err)
	  return nil,err
	}
	if path != ""{	
		err:= client.ChangeDir(path)
		if err!= nil{
			logger.Trace.Println("ftp error changing to directory: ",path,err)
		}	
	}
	reader, err := client.Retr(fileName)
	if err!= nil{
		logger.Trace.Println("ftp error retrieving file: ",fileName,err)
	}	
	
	// Dont forget to close the Reader in the calling function.
	return reader,err
}

