package logger

import (
    "io"
    "log"
)


/* 

Stolen from:

https://www.ardanlabs.com/blog/2013/11/using-log-package-in-go.html 

*/


var (
    Trace   *log.Logger
    Info    *log.Logger
    Warning *log.Logger
    Error   *log.Logger
)

func Init(
    traceHandle io.Writer,
    infoHandle io.Writer,
    warningHandle io.Writer,
    errorHandle io.Writer) {

    Trace = log.New(traceHandle,
        "TRACE: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    Info = log.New(infoHandle,
        "INFO: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    Warning = log.New(warningHandle,
        "WARNING: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    Error = log.New(errorHandle,
        "ERROR: ",
        log.Ldate|log.Ltime|log.Lshortfile)
}

/* Something like this needs to be in the main file


func main() {
    logger.Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)
 
}

Something like this  needs to be in the code to be logged
 
	Trace.Println("I have something standard to say")
    Info.Println("Special Information")
    Warning.Println("There is something you need to know about")
    Error.Println("Something has failed")

*/