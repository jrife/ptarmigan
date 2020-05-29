package plugins

import (
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/plugins/bbolt"
)

var plugins []kv.Plugin

func init() {
	plugins = append(plugins, bbolt.Plugins()...)
}

// Plugin returns the plugin whose name matches the given name.
// It returns nil if no such plugin is found.
func Plugin(name string) kv.Plugin {
	for _, plugin := range plugins {
		if plugin.Name() == name {
			return plugin
		}
	}

	return nil
}

// Plugins lists all the plugins that are available
func Plugins() []kv.Plugin {
	return plugins
}
