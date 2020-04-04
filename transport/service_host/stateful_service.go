package service_host

import "github.com/jrife/ptarmigan/stateful_services"

type StatefulServiceProvidersHost interface {
	StatefulServiceProviders() []stateful_services.StatefulServiceProvider
}
