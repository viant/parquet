package codegen

//Options represents Cli options
type Options struct {
	Source    string `short:"s" long:"sourceURL" description:"source URL"`
	Dest      string `short:"d" long:"destinationURL" description:"destination URL"`
	Type      string `short:"t" long:"struct type" description:"struct type"`
	Pkg       string `short:"p" long:"package" description:"package"`
	OmitEmpty bool   `short: "o long:"omitEmpty" "description:"Omit Empty type"`
}
