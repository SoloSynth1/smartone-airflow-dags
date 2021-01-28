common_pod_args = {
    "is_delete_operator_pod": True,
    "image_pull_policy": "Always",
    "get_logs": False,
}

crawler_pod_args = {
    "affinity": {
        'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        'key': 'cloud.google.com/gke-nodepool',
                        'operator': 'In',
                        'values': ['crawler-pool']
                    }]
                }]
            }
        },
        'podAntiAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': [{
                'labelSelector': {
                    'matchExpressions': [{
                        'key': 'pod-type',
                        'operator': 'In',
                        'values': ['crawler-pod']
                    }]
                },
                'topologyKey': 'kubernetes.io/hostname'
            }]
        }
    },
    "labels": {
        'pod-type': 'crawler-pod'
    },
}