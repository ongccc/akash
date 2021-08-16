package cmd

import (
	"context"
	crd "github.com/ovrclk/akash/pkg/apis/akash.network/v1"
	"github.com/ovrclk/akash/provider/cluster"
	clusterClient "github.com/ovrclk/akash/provider/cluster/kube"
	"github.com/ovrclk/akash/provider/cluster/util"
	"github.com/ovrclk/akash/manifest"
	mtypes "github.com/ovrclk/akash/x/market/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/libs/log"
)


type managedHostname struct {
	lastEvent cluster.HostnameResourceEvent
	presentLease mtypes.LeaseID

	presentServiceName string
	presentExternalPort int32
}

type hostnameOperator struct {
	hostnames map[string]managedHostname

	client cluster.Client

	log log.Logger
}

func (op *hostnameOperator) run(parentCtx context.Context) error {
	// TODO - list through all ingresses labels by Akash provider
	// and then use that data to populate op.hostnames
	ctx, cancel := context.WithCancel(parentCtx)

	connections, err := op.client.GetHostnameDeploymentConnections(ctx)
	if err != nil {
		cancel()
		return err
	}

	for _, conn := range connections {
		leaseID := conn.GetLeaseID()
		hostname := conn.GetHostname()
		entry := managedHostname{
			lastEvent:    nil,
			presentLease: leaseID,
			presentServiceName: conn.GetServiceName(),
			presentExternalPort: conn.GetExternalPort(),
		}

		op.hostnames[hostname] = entry
		op.log.Debug("identified existing hostname connection",
			"hostname", hostname,
			"lease", entry.presentLease,
			"service", entry.presentServiceName,
			"port", entry.presentExternalPort)
	}

	events, err := op.client.ObserveHostnameState(ctx)
	if err != nil {
		cancel()
		return err
	}

	loop:
	for {
		select {
		case <-ctx.Done():
			break loop

		case ev, ok := <- events:
			if !ok {
				op.log.Info("observation stopped")
				break loop
			}
			err = op.applyEvent(ctx, ev)
			if err != nil {
				op.log.Error("failed applying event", "err", err)
				// TODO - fail here ? restart  this operator ?
			}
		}
	}

	cancel()
	return nil
}

func (op *hostnameOperator) applyEvent(ctx context.Context, ev cluster.HostnameResourceEvent) error {
	op.log.Debug("apply event", "event-type", ev.GetEventType(),  "hostname", ev.GetHostname())
	switch ev.GetEventType() {
	case cluster.ProviderResourceDelete:
		// note that on delete the resource might be gone anyways because the namespace is deleted
		return op.applyDeleteEvent(ctx, ev)
	case cluster.ProviderResourceAdd, cluster.ProviderResourceUpdate:
		return op.applyAddOrUpdateEvent(ctx, ev)
	default:
		// TODO - ????
		panic(ev.GetEventType())
	}

}

func (op *hostnameOperator) applyDeleteEvent(ctx context.Context, ev cluster.HostnameResourceEvent) error {
	leaseID := ev.GetLeaseID()
	err := op.client.RemoveHostnameFromDeployment(ctx, ev.GetHostname(), leaseID, true)

	if err == nil {
		delete(op.hostnames, ev.GetHostname())
	}

	return err
}

func (op *hostnameOperator) applyAddOrUpdateEvent(ctx context.Context, ev cluster.HostnameResourceEvent) error {
	// Locate the matchin service name & expose directive in the manifest CRD
	found, manifestGroup, err := op.client.GetManifestGroup(ctx, ev.GetLeaseID())
	if err != nil {
		return err
	}
	if !found {
		panic("no manifest found") // poll to wait for data since etcd is eventually consistent
	}


	var selectedService crd.ManifestService
	for _, service := range manifestGroup.Services {
		if service.Name == ev.GetServiceName() {
			selectedService = service
			break
		}
	}

	if selectedService.Count == 0 {
		panic("no service found")
	}

	var selectedExpose crd.ManifestServiceExpose
	for _, expose := range selectedService.Expose {
		if ! expose.Global {
			continue
		}

		if ev.GetExternalPort() == uint32(util.ExposeExternalPort(manifest.ServiceExpose{
			Port:         expose.Port,
			ExternalPort: expose.ExternalPort,
		})) {
			selectedExpose = expose
			break
		}
	}

	if selectedExpose.Port == 0 {
		panic("no expose found")
	}

	leaseID := ev.GetLeaseID()

	op.log.Debug("connecting",
		"hostname", ev.GetHostname(),
		"lease", leaseID,
		"service", ev.GetServiceName(),
		"externalPort", ev.GetExternalPort())
	entry, exists := op.hostnames[ev.GetHostname()]

	isSameLease := false
	if exists {
		isSameLease = entry.presentLease.Equals(leaseID)
	} else {
		isSameLease = true
	}

	directive := cluster.ConnectHostnameToDeploymentDirective{
		Hostname:    ev.GetHostname(),
		LeaseID:     leaseID,
		ServiceName: ev.GetServiceName(),
		ServicePort: int32(ev.GetExternalPort()),
		ReadTimeout: selectedExpose.HttpOptions.ReadTimeout,
		SendTimeout: selectedExpose.HttpOptions.SendTimeout,
		NextTimeout: selectedExpose.HttpOptions.NextTimeout,
		MaxBodySize: selectedExpose.HttpOptions.MaxBodySize,
		NextTries:   selectedExpose.HttpOptions.NextTries,
		NextCases:   selectedExpose.HttpOptions.NextCases,
	}

	if isSameLease {
		// Check to see if port or service name is different
		changed := !exists || uint32(entry.presentExternalPort) != ev.GetExternalPort() || entry.presentServiceName != ev.GetServiceName()
		if changed {
			op.log.Debug("Updating ingress")
			// Update or create the existing ingress
			err = op.client.ConnectHostnameToDeployment(ctx, directive)
		}
	} else {
		op.log.Debug("Swapping ingress to new deployment")
		//  Delete the ingress in one namespace and recreate it in the correct one
		err = op.client.RemoveHostnameFromDeployment(ctx, ev.GetHostname(), entry.presentLease, false)
		if err == nil {
			err = op.client.ConnectHostnameToDeployment(ctx, directive)
		}
	}

	if err == nil { // Update sored entry if everything went OK
		entry.presentLease = leaseID
		entry.lastEvent = ev
		op.hostnames[ev.GetHostname()] = entry
	}

	return err
}

func doHostnameOperator(cmd *cobra.Command) error {
	ns := "lease"
	settings := clusterClient.Settings{
		DeploymentServiceType:          "",
		DeploymentIngressStaticHosts:   false,
		DeploymentIngressDomain:        "",
		DeploymentIngressExposeLBHosts: false,
		ClusterPublicHostname:          "",
		NetworkPoliciesEnabled:         false,
		CPUCommitLevel:                 0,
		MemoryCommitLevel:              0,
		StorageCommitLevel:             0,
		ConfigPath:                     "",
		DeploymentRuntimeClass:         "",
	}
	log := openLogger()
	client, err  := clusterClient.NewClient(log, ns, settings)
	if err != nil {
		return err
	}

	op := hostnameOperator{
		hostnames: make(map[string]managedHostname),
		client:    client,
		log: log,
	}

	return op.run(cmd.Context())
}