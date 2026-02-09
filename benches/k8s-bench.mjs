#!/usr/bin/env zx

/**
 * Kubernetes benchmark runner for silo-bench
 * 
 * Creates a Kubernetes Pod running the silo docker container and executes
 * silo-bench with the provided arguments.
 * 
 * Usage:
 *   npx zx benches/k8s-bench.mjs --address http://silo-service:7450 --workers 16
 *   npx zx benches/k8s-bench.mjs -a http://silo-service:7450 -w 16 -d 60
 *   npx zx benches/k8s-bench.mjs --address http://silo-service:7450 --keep-pod
 * 
 * Environment variables:
 *   SILO_IMAGE - Docker image to use (default: silo:latest)
 *   KUBECTL_NAMESPACE - Kubernetes namespace (default: default)
 * 
 * Special arguments:
 *   --namespace <namespace> - Override namespace for this run
 *   --keep-pod - Don't delete the pod after completion (useful for debugging)
 *   --no-structured-logging - Disable structured JSON logging (enabled by default)
 */

import { $, argv, fs } from 'zx';
import { randomUUID } from 'crypto';

// Parse arguments - everything after script name goes to silo-bench
const benchArgs = process.argv.slice(2);

// Generate a unique pod name
const podName = `silo-bench-${randomUUID().split('-')[0]}`;

// Default image name (can be overridden with SILO_IMAGE env var)
const image = process.env.SILO_IMAGE || 'silo:latest';

// Default namespace (can be overridden with KUBECTL_NAMESPACE env var or --namespace)
let namespace = process.env.KUBECTL_NAMESPACE || 'default';
const namespaceIndex = benchArgs.indexOf('--namespace');
if (namespaceIndex !== -1 && benchArgs[namespaceIndex + 1]) {
  namespace = benchArgs[namespaceIndex + 1];
  // Remove --namespace and its value from benchArgs
  benchArgs.splice(namespaceIndex, 2);
}

// Check if we should keep the pod after completion
const keepPod = benchArgs.includes('--keep-pod');
if (keepPod) {
  const index = benchArgs.indexOf('--keep-pod');
  benchArgs.splice(index, 1);
}

// Handle structured logging flag
const noStructuredLogging = benchArgs.includes('--no-structured-logging');
if (noStructuredLogging) {
  const index = benchArgs.indexOf('--no-structured-logging');
  benchArgs.splice(index, 1);
} else if (!benchArgs.includes('--structured-logging')) {
  // Add --structured-logging by default if not already present and not disabled
  benchArgs.push('--structured-logging');
}

console.log(`Creating Kubernetes Pod: ${podName}`);
console.log(`Image: ${image}`);
console.log(`Namespace: ${namespace}`);
console.log(`silo-bench args: ${benchArgs.join(' ')}`);

// Build the silo-bench command with arguments
const benchCommand = ['silo-bench', ...benchArgs].join(' ');

// Create the pod manifest
const podManifest = {
  apiVersion: 'v1',
  kind: 'Pod',
  metadata: {
    name: podName,
    namespace: namespace,
    labels: {
      app: 'silo-bench',
      'silo.dev/component': 'benchmark',
    },
  },
  spec: {
    restartPolicy: 'Never',
    containers: [
      {
        name: 'silo-bench',
        image: image,
        command: ['/bin/sh', '-c', benchCommand],
        resources: {
          requests: {
            memory: '256Mi',
            cpu: '100m',
          },
          limits: {
            memory: '2Gi',
            cpu: '2000m',
          },
        },
      },
    ],
  },
};

// Write manifest to temp file
const manifestPath = `/tmp/${podName}-manifest.json`;
await fs.writeFile(manifestPath, JSON.stringify(podManifest, null, 2));

try {
  // Create the pod
  await $`kubectl apply -f ${manifestPath}`;
  console.log(`Pod ${podName} created successfully`);

  // Wait for pod to be ready
  console.log('Waiting for pod to be ready...');
  await $`kubectl wait --for=condition=Ready pod/${podName} -n ${namespace} --timeout=60s`;

  // Stream logs
  console.log('\n=== silo-bench output ===\n');
  try {
    await $`kubectl logs -f pod/${podName} -n ${namespace}`;
  } catch (error) {
    // Logs might exit with non-zero if pod terminates, which is fine
    if (error.exitCode !== 0) {
      // Try to get logs one more time in case of streaming issues
      await $`kubectl logs pod/${podName} -n ${namespace}`;
    } else {
      throw error;
    }
  }

  // Get pod status
  const status = await $`kubectl get pod/${podName} -n ${namespace} -o jsonpath='{.status.phase}'`.quiet();
  console.log(`\nPod finished with status: ${status.stdout.trim()}`);

  // Clean up unless --keep-pod was specified
  if (!keepPod) {
    console.log(`Cleaning up pod ${podName}...`);
    await $`kubectl delete pod/${podName} -n ${namespace}`;
    console.log('Pod deleted');
  } else {
    console.log(`Pod ${podName} kept running (use --keep-pod to preserve)`);
    console.log(`View logs with: kubectl logs pod/${podName} -n ${namespace}`);
  }
} catch (error) {
  console.error('Error running benchmark:', error.message);

  // Try to get pod logs for debugging
  try {
    console.log('\n=== Pod logs (for debugging) ===');
    await $`kubectl logs pod/${podName} -n ${namespace}`;
  } catch (logError) {
    // Ignore if pod doesn't exist or logs aren't available
  }

  // Clean up on error unless --keep-pod was specified
  if (!keepPod) {
    try {
      await $`kubectl delete pod/${podName} -n ${namespace} --ignore-not-found=true`;
    } catch (deleteError) {
      // Ignore delete errors
    }
  }

  process.exit(1);
} finally {
  // Clean up temp manifest file
  try {
    await fs.unlink(manifestPath);
  } catch (error) {
    // Ignore cleanup errors
  }
}


