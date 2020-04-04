package clients

import (
	stateful_services_client "github.com/jrife/ptarmigan/stateful_services/external/clients"
)

type StatefulServicesClient interface {
	StatefulServiceProvider(name string) StatefulServiceProviderClient
}

type StatefulServiceProviderClient interface {
	stateful_services_client.StatefulServiceProviderClient
}
