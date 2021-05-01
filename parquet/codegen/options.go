package codegen

import "errors"

//Options represents Cli options
type Options struct {
	Source    string `short:"s" long:"sourceURL" description:"source URL"`
	Dest      string `short:"d" long:"destinationURL" description:"destination URL"`
	Type      string `short:"t" long:"struct type" description:"struct type"`
	OmitEmpty bool   `short:"o" long:"omitEmpty" description:"Omit Empty type"`
}

func (o Options) Validate() error {
	if o.Source == "" {
		return errors.New("source is empty")
	}
	if o.Dest == "" {
		return errors.New("dest is empty")
	}
	if o.Type == "" {
		return errors.New("type is empty")
	}
	return nil
}
