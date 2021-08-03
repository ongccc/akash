package cmd

import (
	"context"
	"github.com/ovrclk/akash/manifest"
	akashtypes "github.com/ovrclk/akash/pkg/apis/akash.network/v1"
	"github.com/ovrclk/akash/provider/cluster"
	clusterClient "github.com/ovrclk/akash/provider/cluster/kube"
	"github.com/ovrclk/akash/provider/cluster/util"
	mtypes "github.com/ovrclk/akash/x/market/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/libs/log"
	"strings"
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
	leaseID := mtypes.LeaseID{} // TODO - get real value here
	err := op.client.RemoveHostnameFromDeployment(ctx, ev.GetHostname(), leaseID, true)

	if err == nil {
		delete(op.hostnames, ev.GetHostname())
	}

	return err
}

func (op *hostnameOperator) applyAddOrUpdateEvent(ctx context.Context, ev cluster.HostnameResourceEvent) error {

	leaseID := ev.GetLeaseID()
	// Fetch manifest group for the deployment
	found, mgroup, err := op.client.GetManifestGroup(ctx, leaseID)
	if err != nil {
		return err
	}

	if !found {
		panic("no such manifest found") // TODO - return an error
	}

	var selectedService akashtypes.ManifestService
	for _, service := range mgroup.Services {
		autoIngressHost := util.IngressHost(leaseID, service.Name)
		op.log.Debug("checking for match", "candidate", autoIngressHost)

		// TODO - check that this ends with c.settings.DeploymentIngressDomain
		if strings.HasPrefix(ev.GetHostname(), autoIngressHost + ".") {
			selectedService = service
		} else {
			for _, serviceExpose := range service.Expose {
				for _, host := range serviceExpose.Hosts {
					op.log.Debug("checking for match", "candidate", host)
					if host == ev.GetHostname() {
						selectedService = service
					}
				}
			}
		}
		if len(selectedService.Name) != 0 {
			break
		}
	}
	// TODO - check to see if none matched
	if len(selectedService.Name) == 0{
		panic("no service selected")
	}

	externalPort := int32(-1)
	for _, expose := range selectedService.Expose {
		serviceExpose := manifest.ServiceExpose{
			Port:         expose.Port,
			ExternalPort: expose.ExternalPort,
			Global:       expose.Global,
			Proto: manifest.ServiceProtocol(expose.Proto),
		}
		if !util.ShouldBeIngress(serviceExpose){
			continue
		}
		externalPort = int32(expose.ExternalPort)
		break
	}
	if externalPort == -1 {
		panic("no external port selected")
	}

	op.log.Debug("connecting",
		"hostname", ev.GetHostname(),
		"lease", leaseID,
		"service", selectedService.Name,
		"externalPort", externalPort)
	entry, exists := op.hostnames[ev.GetHostname()]

	isSameLease := false
	if exists {
		isSameLease = entry.presentLease.Equals(leaseID)
	} else {
		isSameLease = true
	}

	if isSameLease {
		// Check to see if port or service name is different
		changed := !exists || entry.presentExternalPort != externalPort || entry.presentServiceName != selectedService.Name
		if changed {
			op.log.Debug("Updating ingress")
			// Update or create the existing ingress
			err = op.client.ConnectHostnameToDeployment(ctx, ev.GetHostname(), leaseID, selectedService.Name, externalPort)
		}
	} else {
		op.log.Debug("Swapping ingress to new deployment")
		// TODO - Delete the ingress in one namespace and recreate it in the correct one
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