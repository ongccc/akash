package cmd

import (
	"context"
	"github.com/ovrclk/akash/manifest"
	"github.com/ovrclk/akash/provider/cluster"
	clusterClient "github.com/ovrclk/akash/provider/cluster/kube"
	ctypes "github.com/ovrclk/akash/provider/cluster/types"
	"github.com/ovrclk/akash/provider/cluster/util"
	dtypes "github.com/ovrclk/akash/x/deployment/types"
	mtypes "github.com/ovrclk/akash/x/market/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/libs/log"
)

type dummy interface {
	ObserveHostnameState(ctx context.Context) (<- chan cluster.HostnameResourceEvent, error)
	GetDeployments(ctx context.Context, dID dtypes.DeploymentID) ([]ctypes.Deployment, error)
	ConnectHostnameToDeployment(ctx context.Context, hostname string, leaseID mtypes.LeaseID, serviceName string, servicePort int32) error
	GetHostnameDeploymentConnections(ctx context.Context) ([]cluster.LeaseIdAndHostname, error)
}

type managedHostname struct {
	lastEvent cluster.HostnameResourceEvent
	presentLease mtypes.LeaseID
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

	connections, err := op.client.(dummy).GetHostnameDeploymentConnections(ctx)
	if err != nil {
		return err
	}

	for _, conn := range connections {
		leaseID := conn.GetLeaseID()
		hostname := conn.GetHostname()
	}


	events, err := op.client.(dummy).ObserveHostnameState(ctx)
	if err != nil {
		cancel()
		return err
	}

	for {
		select {
		case <-ctx.Done():
			break

		case ev, ok := <- events:
			if !ok {
				break
			}
			op.applyEvent(ctx, ev)
		}
	}
}

func (op *hostnameOperator) applyEvent(ctx context.Context, ev cluster.HostnameResourceEvent) error {
	op.log.Debug("apply event %s for %q", ev.GetEventType(), ev.GetHostname())
	switch ev.GetEventType() {
	case cluster.ProviderResourceDelete:
		// TODO
	case cluster.ProviderResourceAdd, cluster.ProviderResourceUpdate:
		return op.applyAddOrUpdateEvent(ctx, ev)
	default:
		// TODO - ????
		panic("boom")
	}

	return nil
}

func (op *hostnameOperator) applyAddOrUpdateEvent(ctx context.Context, ev cluster.HostnameResourceEvent) error {
	// Fetch manifests for the deployment
	dID := dtypes.DeploymentID{
		Owner: ev.GetOwner().String(),
		DSeq:  ev.GetDeploymentSequence(),
	}
	manifests, err := op.client.(dummy).GetDeployments(ctx, dID)
	if err != nil {
		return err
	}

	selection := -1
	var selectedService manifest.Service
	for i, manifestEntry := range manifests {

		containsHost := false
		var serviceWithHost manifest.Service
		for _, service := range manifestEntry.ManifestGroup().Services {
			for _, serviceExpose := range service.Expose {
				for _, host := range serviceExpose.Hosts {
					if host == ev.GetHostname() {
						containsHost = true
						serviceWithHost = service
					}
				}
			}
		}
		if !containsHost {
			continue
		}

		lID := manifestEntry.LeaseID()
		isLatest := selection == -1 || manifests[selection].LeaseID().OSeq < lID.OSeq
		if isLatest {
			selection = i
			selectedService = serviceWithHost
		}
	}
	// Ingress must exist in the same namespace as the service that it refers to
	if selection == -1 {
		panic("no such manifest found") // TODO - return an error
	}

	selectedManifest := manifests[selection]

	var externalPort int32
	for _, expose := range selectedService.Expose {
		if !util.ShouldBeIngress(expose){
			continue
		}

		externalPort = util.ExposeExternalPort(expose)
		break
	}

	entry, exists := op.hostnames[ev.GetHostname()]

	isSameLease := false
	if exists {
		isSameLease = entry.presentLease == selectedManifest.LeaseID()
	} else {
		isSameLease = true
	}

	if isSameLease {
		// Update or create the existing ingress
		err = op.client.(dummy).ConnectHostnameToDeployment(ctx, ev.GetHostname(), selectedManifest.LeaseID(), selectedService.Name, externalPort)
	} else {
		// Delete the ingress in one namespace and recreate it in the correct one

	}

	if err == nil {
		entry.presentLease = selectedManifest.LeaseID()
		entry.lastEvent = ev
		op.hostnames[ev.GetHostname()] = entry
	}

	return nil
}

/**
func (op *hostnameOperator) ingressRules(hostname string, kubeServiceName string, kubeServicePort int32) []netv1.IngressRule{
	// for some reason we need to pass a pointer to this
	pathTypeForAll := netv1.PathTypePrefix
	ruleValue := netv1.HTTPIngressRuleValue{
		Paths: []netv1.HTTPIngressPath{{
			Path:     "/",
			PathType: &pathTypeForAll,
			Backend:  netv1.IngressBackend{
				Service:  &netv1.IngressServiceBackend{
					Name: kubeServiceName,
					Port: netv1.ServiceBackendPort{
						Number: kubeServicePort,
					},
				},
			},
		},},
	}

	return []netv1.IngressRule{{
		Host:             hostname,
		IngressRuleValue: netv1.IngressRuleValue{HTTP: &ruleValue},
		},
	}

}
**/
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