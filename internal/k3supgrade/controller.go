package k3supgrade

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The system-upgrade-controller that runs the Plans. The maintenance overlay
// (deploy/maintenance) installs it with ZERO replicas, with its settings,
// namespace, service account and RBAC. The agent never creates or changes
// the controller: it may only scale it (the deployments/scale subresource,
// by name). It scales it to one replica only while maintenance is on and an
// operation is active, so no privileged upgrade job can start without the
// agent, and an agent that is taken over cannot change the controller's pod.
//
// The templates below are the source of the overlay file
// deploy/maintenance/system-upgrade-controller.yaml; a test keeps the two
// equal. Pinned to rancher/system-upgrade-controller v0.20.2 (2026-09-23),
// by digest. The pod spec mirrors the upstream release manifest
// (system-upgrade-controller.yaml of that release). The controller creates
// its Plan CRD itself when it starts.
const (
	// ControllerName is the Deployment name (upstream).
	ControllerName = "system-upgrade-controller"
	// ControllerImage is v0.20.2, multi-arch index digest, resolved from
	// Docker Hub on 2026-11-06.
	ControllerImage = "rancher/system-upgrade-controller:v0.20.2@sha256:61b68d4372baf7ae90fb0f879feea1c082381a8b7547cc5ec85d393c497b11cb"
	// KubectlImage runs the cordon step of each upgrade job. The upstream
	// default of v0.20.2, pinned by its index digest (2026-11-06).
	KubectlImage = "rancher/kubectl:v1.30.3@sha256:86264eefaba5ffc7df2ab290f02dc4c79befd29ff9dfc6d0de630e71d6bfd375"
	// ControllerEnvConfigMap holds the controller settings (upstream name).
	ControllerEnvConfigMap = "default-controller-env"
	// controllerPodLabel selects the controller pods (and no job pod: the
	// controller does not put this label on its jobs).
	controllerPodLabel = "app.kubernetes.io/name"
	// specHashAnnotation records the hash of the spec this executor wrote,
	// so a reconcile updates an object only when the agent's own template
	// changed (the apiserver adds defaults that a field compare would see
	// as drift).
	specHashAnnotation = "kubehz.cloud/spec-hash"
)

// Controller settings. Two differ from upstream, on purpose:
//   - SYSTEM_UPGRADE_JOB_BACKOFF_LIMIT 0 (upstream 99): retries are off
//     (design 4.3 step 5). A failed job fails the operation.
//   - SYSTEM_UPGRADE_JOB_TTL_SECONDS_AFTER_FINISH 86400 (upstream 900): the
//     controller re-creates a job for a node only after the failed job is
//     gone. One day gives the agent time to see the failure and remove the
//     Plans, also after a restart.
//
// The job deadline (900 s) stays: it ends a job that cannot pull its image,
// which then fails the operation.
func controllerEnv() map[string]string {
	return map[string]string{
		"SYSTEM_UPGRADE_CONTROLLER_DEBUG":             "false",
		"SYSTEM_UPGRADE_CONTROLLER_LEADER_ELECT":      "true",
		"SYSTEM_UPGRADE_CONTROLLER_THREADS":           "2",
		"SYSTEM_UPGRADE_JOB_ACTIVE_DEADLINE_SECONDS":  "900",
		"SYSTEM_UPGRADE_JOB_BACKOFF_LIMIT":            "0",
		"SYSTEM_UPGRADE_JOB_IMAGE_PULL_POLICY":        "Always",
		"SYSTEM_UPGRADE_JOB_KUBECTL_IMAGE":            KubectlImage,
		"SYSTEM_UPGRADE_JOB_PRIVILEGED":               "true",
		"SYSTEM_UPGRADE_JOB_TTL_SECONDS_AFTER_FINISH": "86400",
		"SYSTEM_UPGRADE_PLAN_POLLING_INTERVAL":        "15m",
	}
}

func managedLabels() map[string]string {
	return map[string]string{LabelManagedBy: ManagedBy}
}

// ControllerConfigMap is the controller's settings ConfigMap.
func ControllerConfigMap() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControllerEnvConfigMap,
			Namespace: Namespace,
			Labels:    managedLabels(),
		},
		Data: controllerEnv(),
	}
}

// ControllerDeployment is the controller Deployment with the given replica
// count (0 or 1).
func ControllerDeployment(replicas int32) *appsv1.Deployment {
	podLabels := map[string]string{
		"app.kubernetes.io/component":  "controller",
		controllerPodLabel:             ControllerName,
		"upgrade.cattle.io/controller": ControllerName,
		LabelManagedBy:                 ManagedBy,
	}
	labels := managedLabels()
	labels[controllerPodLabel] = ControllerName
	fieldEnv := func(name, path string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: path},
		}}
	}
	hostPath := func(name, path string) corev1.Volume {
		t := corev1.HostPathDirectoryOrCreate
		return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: path, Type: &t},
		}}
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControllerName,
			Namespace: Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"upgrade.cattle.io/controller": ControllerName,
			}},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName: ServiceAccount,
					Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
								NodeSelectorTerms: []corev1.NodeSelectorTerm{{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: ControlPlaneLabel, Operator: corev1.NodeSelectorOpExists},
										{Key: "kubernetes.io/os", Operator: corev1.NodeSelectorOpIn, Values: []string{"linux"}},
									},
								}},
							},
						},
					},
					Tolerations: []corev1.Toleration{
						{Key: "CriticalAddonsOnly", Operator: corev1.TolerationOpExists},
						{Key: "node-role.kubernetes.io/master", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
						{Key: "node-role.kubernetes.io/controlplane", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
						{Key: ControlPlaneLabel, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
						{Key: "node-role.kubernetes.io/etcd", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
					},
					Containers: []corev1.Container{{
						Name:            ControllerName,
						Image:           ControllerImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env: []corev1.EnvVar{
							fieldEnv("SYSTEM_UPGRADE_CONTROLLER_NAME", "metadata.labels['upgrade.cattle.io/controller']"),
							fieldEnv("SYSTEM_UPGRADE_CONTROLLER_NAMESPACE", "metadata.namespace"),
							fieldEnv("SYSTEM_UPGRADE_CONTROLLER_NODE_NAME", "spec.nodeName"),
						},
						EnvFrom: []corev1.EnvFromSource{{
							ConfigMapRef: &corev1.ConfigMapEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: ControllerEnvConfigMap},
							},
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							RunAsGroup:               ptr(int64(65534)),
							RunAsNonRoot:             ptr(true),
							RunAsUser:                ptr(int64(65534)),
							SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "etc-ssl", MountPath: "/etc/ssl", ReadOnly: true},
							{Name: "etc-pki", MountPath: "/etc/pki", ReadOnly: true},
							{Name: "etc-ca-certificates", MountPath: "/etc/ca-certificates", ReadOnly: true},
							{Name: "tmp", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						hostPath("etc-ssl", "/etc/ssl"),
						hostPath("etc-pki", "/etc/pki"),
						hostPath("etc-ca-certificates", "/etc/ca-certificates"),
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
}

func ptr[T any](v T) *T { return &v }
