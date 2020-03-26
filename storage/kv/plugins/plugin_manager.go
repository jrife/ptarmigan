package plugins

import (
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/plugins/bbolt"
)

// KVPluginManager lets a consumer
// retrieve the KV storage plugin
// by name
type KVPluginManager struct {
	plugins []kv.Plugin
}

// NewKVPluginManager returns a KVPluginManager
// that is loaded with all supported plugins.
func NewKVPluginManager() *KVPluginManager {
	plugins := []kv.Plugin{}

	plugins = append(plugins, bbolt.Plugins()...)

	return &KVPluginManager{
		plugins: plugins,
	}
}

// Plugin returns the plugin whose name matches the given name.
// It returns nil if no such plugin is found.
func (pluginManager *KVPluginManager) Plugin(name string) kv.Plugin {
	for _, plugin := range pluginManager.plugins {
		if plugin.Name() == name {
			return plugin
		}
	}

	return nil
}

func (pluginManager *KVPluginManager) Plugins() []kv.Plugin {
	return pluginManager.plugins
}
