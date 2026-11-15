import time
import uuid
import threading
import queue
from kubernetes import client, config, watch

# Domain Definitions from crd_definition.yaml
DOMAIN = "cosc3657.tu.ca"
VERSION = "v1alpha1"
PLURAL = "resilientdeployments"

# Thread-safe queue to register API events
event_queue = queue.Queue()

def reconcile(name, namespace, core_v1, custom_api):
    #Core Idempotent Reconciliation Logic: f(f(x)) = f(x)
    #Computes State Drift and aligns Observed State with Desired State.

    try:
        # 1. Observe Desired State (N_desired)
        cr = custom_api.get_namespaced_custom_object(
            group=DOMAIN, version=VERSION, namespace=namespace, plural=PLURAL, name=name
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            # The Custom Resource was deleted. 
            # In a full deployment, we would cascade deletion to managed pods here.
            return
        raise e

    # Extract the Desired State (N_desired) and container image from the declarative manifest, 
    # defaulting to 1 replica if the user did not explicitly specify a count.
    desired_replicas = cr['spec'].get('replicas', 1)
    image = cr['spec'].get('image')

    # 2. Observe Live State (N_observed)
    label_selector = f"app.kubernetes.io/managed-by={name}"
    pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
    
    # Filter active pods (exclude those already marked for termination)
    active_pods = [p for p in pods.items if p.metadata.deletion_timestamp is None]
    observed_replicas = len(active_pods)

    # 3. Compute State Drift (Delta)
    drift = desired_replicas - observed_replicas
    
    print(f"[RECONCILE LOOP] Resource: {name} | Desired: {desired_replicas} | Observed: {observed_replicas} | Drift: {drift}")

    #Reconcile Actions based on Delta
    #3.1 d>0
    if drift > 0:
        print(f"Scale-Up Event: Provisioning {drift} pod(s) to resolve drift...")
        for _ in range(drift):
            pod_name = f"{name}-{uuid.uuid4().hex[:6]}"
            pod_manifest = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "labels": {
                        "app.kubernetes.io/managed-by": name
                    }
                },
                "spec": {
                    "containers": [{
                        "name": "app",
                        "image": image
                    }]
                }
            }
            core_v1.create_namespaced_pod(namespace=namespace, body=pod_manifest)
    #3.2 d<0
    elif drift < 0:
        excess = abs(drift)
        print(f"Scale-Down Event: Terminating {excess} pod(s) to resolve drift...")
        for i in range(excess):
            pod_to_delete = active_pods[i].metadata.name
            core_v1.delete_namespaced_pod(name=pod_to_delete, namespace=namespace)
    #if d = 0 then it passes

    #Equilibrium validation
    print(f"EQUILIBRIUM REACHED: System state matches target spec for '{name}'.\n")


def watch_crds():
    #Stream API events for ResilientDeployments (Declarative Intent Changes)
    config.load_kube_config()
    custom_api = client.CustomObjectsApi()
    w = watch.Watch()
    
    for event in w.stream(custom_api.list_cluster_custom_object, group=DOMAIN, version=VERSION, plural=PLURAL):
        # Trigger reconciliation when a CR is created or its spec is updated (Scale Mutated)
        if event['type'] in ['ADDED', 'MODIFIED']:
            cr = event['object']
            event_queue.put((cr['metadata']['name'], cr['metadata']['namespace']))


def watch_pods():
    #Stream API events for Pods (Crash-stop failure detection)
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    w = watch.Watch()
    
    for event in w.stream(core_v1.list_pod_for_all_namespaces):
        # Only trigger reconciliation on DELETED to detect simulated node/process crashes
        # Ignoring ADDED/MODIFIED prevents redundant loop execution while pods spin up.
        if event['type'] == 'DELETED':
            pod = event['object']
            labels = pod.metadata.labels or {}
            
            # Only track pods managed by our controller
            if "app.kubernetes.io/managed-by" in labels:
                cr_name = labels["app.kubernetes.io/managed-by"]
                event_queue.put((cr_name, pod.metadata.namespace))


def main():
    print("=== COSC 3657 Custom Kubernetes Reconciler Started ===\n")
    
    # Initialize connection to local kind cluster
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    custom_api = client.CustomObjectsApi()

    # Launch asynchronous watch streams
    threading.Thread(target=watch_crds, daemon=True).start()
    threading.Thread(target=watch_pods, daemon=True).start()

    # Continuous control loop processing events
    while True:
        # Block until an architecture event occurs
        name, namespace = event_queue.get()
        
        # Micro-sleep ensures Kubernetes API registers object changes before read-back
        time.sleep(0.5) 
        
        reconcile(name, namespace, core_v1, custom_api)
        event_queue.task_done()

if __name__ == '__main__':
    main()