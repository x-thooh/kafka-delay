package app

// Instance is an instance of a service in a discovery system.
type Instance struct {
	// Version is the version of the compiled.
	Version string `json:"version"`
	// Metadata is the kv pair metadata associated with the service instance.
	Metadata map[string]string `json:"metadata"`
}
