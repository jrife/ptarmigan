package clients

import "github.com/jrife/ptarmigan/stateful_services"

type StatefulServiceProviderClient interface {
	stateful_services.StatefulServiceProvider
}
