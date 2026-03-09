package registry

import (
	"log"

	"github.com/tbright/heimdall/internal/config"
	"github.com/tbright/heimdall/internal/plugin"
	"github.com/tbright/heimdall/internal/plugin/webhook"
)

// BuildPlugins constructs Plugin instances from config definitions.
// Unknown plugin types are logged and skipped. Plugins whose construction fails are logged and skipped.
func BuildPlugins(defs []config.PluginDef, logger *log.Logger) []plugin.Plugin {
	var plugins []plugin.Plugin

	for _, def := range defs {
		switch def.Type {
		case "webhook":
			p, err := webhook.NewWebhookPlugin(def.Name, def.Config)
			if err != nil {
				logger.Printf("skipping plugin %q: %v", def.Name, err)
				continue
			}
			plugins = append(plugins, p)
		default:
			logger.Printf("skipping plugin %q: unknown plugin type %q", def.Name, def.Type)
		}
	}

	return plugins
}
