package resources

import "embed"

//go:embed collections/default.json queries/default.json
var Defaults embed.FS
