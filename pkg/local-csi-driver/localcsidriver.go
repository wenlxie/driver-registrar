package local_csi_driver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	"github.com/kubernetes-csi/driver-registrar/pkg/connection"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/util/retry"
)

const (
	driverCapacityKey = "csi.volume.kubernetes.io/kubernetes.io.csi.local"
	vgName            = "vg10000"
)

func LocalCSIDriverGetCapacity(csiConn connection.CSIConnection, ctx context.Context) (int64, error) {
	// Need discuss with upstream for this,
	// It is better  to pass the vg name to the capacity request.
	// For different node, they may have different vg name
	para := map[string]string{
		"volume-group": vgName,
	}

	req := &csi.GetCapacityRequest{
		VolumeCapabilities: nil,
		Parameters:         para,
	}
	// The request may includes many vgs, but the response is the capacity with type int64
	// This does not make sense.
	csiDriverNodeCapacity, err := csiConn.GetCapacity(ctx, req)
	if err != nil {
		glog.Error(err.Error())
		return int64(0), err
	}
	glog.V(2).Infof("CSI driver node capacity:%+V", csiDriverNodeCapacity)
	return csiDriverNodeCapacity.AvailableCapacity, err
}

// Fetches Kubernetes node API object corresponding to k8sNodeName.
// If the csiDriverName and csiDriverNodeId are not present in the node
// annotation, this method adds it.
func GetVerifyAndAddNodeId(
	k8sNodeName string,
	k8sNodesClient corev1.NodeInterface,
	csiDriverName string,
	csiDriverCapacity string) error {
	// Add or update annotation on Node object
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Retrieve the latest version of Node before attempting update, so that
		// existing changes are not overwritten. RetryOnConflict uses
		// exponential backoff to avoid exhausting the apiserver.
		result, getErr := k8sNodesClient.Get(k8sNodeName, metav1.GetOptions{})
		if getErr != nil {
			glog.Errorf("Failed to get latest version of Node: %v", getErr)
			return getErr // do not wrap error
		}

		var previousAnnotationValue string
		if result.ObjectMeta.Annotations != nil {
			previousAnnotationValue =
				result.ObjectMeta.Annotations[driverCapacityKey]
			glog.V(3).Infof(
				"previousAnnotationValue=%q", previousAnnotationValue)
		}

		existingVgCapacityMap := map[string]string{}
		if previousAnnotationValue != "" {
			// Parse previousAnnotationValue as JSON
			if err := json.Unmarshal([]byte(previousAnnotationValue), &existingVgCapacityMap); err != nil {
				return fmt.Errorf(
					"Failed to parse node's %q annotation value (%q) err=%v",
					driverCapacityKey,
					previousAnnotationValue,
					err)
			}
		}

		if val, ok := existingVgCapacityMap[vgName]; ok {
			if val == csiDriverCapacity {
				// Value already exists in node annotation, nothing more to do
				glog.V(1).Infof(
					"The key value {%q: %q} alredy eixst in node %q annotation, no need to update: %v",
					csiDriverName,
					csiDriverCapacity,
					driverCapacityKey,
					previousAnnotationValue)
				return nil
			}
		}

		// Add/update annotation value
		existingVgCapacityMap[vgName] = csiDriverCapacity
		jsonObj, err := json.Marshal(existingVgCapacityMap)
		if err != nil {
			return fmt.Errorf(
				"Failed while trying to add key value {%q: %q} to node %q annotation. Existing value: %v",
				vgName,
				csiDriverCapacity,
				driverCapacityKey,
				previousAnnotationValue)
		}

		result.ObjectMeta.Annotations = cloneAndAddAnnotation(
			result.ObjectMeta.Annotations,
			driverCapacityKey,
			string(jsonObj))
		_, updateErr := k8sNodesClient.Update(result)
		if updateErr == nil {
			glog.V(1).Infof(
				"Updated node %q successfully for CSI driver %q",
				k8sNodeName,
				csiDriverName)
		} else {
			glog.V(1).Infof(
				"Updated node %q error for CSI driver %q : %v",
				k8sNodeName,
				csiDriverName,
				updateErr)
		}
		return updateErr // do not wrap error
	})
	if retryErr != nil {
		return fmt.Errorf("Node update failed: %v", retryErr)
	}
	return nil
}

// Clones the given map and returns a new map with the given key and value added.
// Returns the given map, if annotationKey is empty.
func cloneAndAddAnnotation(
	annotations map[string]string,
	annotationKey,
	annotationValue string) map[string]string {
	if annotationKey == "" {
		// Don't need to add an annotation.
		return annotations
	}
	// Clone.
	newAnnotations := map[string]string{}
	for key, value := range annotations {
		newAnnotations[key] = value
	}
	newAnnotations[annotationKey] = annotationValue
	return newAnnotations
}
