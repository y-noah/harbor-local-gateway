// The project pool and explicit legacy pools require Docker/services and real UID ownership.
// The compute-runner tag must never route private-service or root-dependent jobs.
export function dockerRunnerTag(value='dashboard-docker'){
  if(!['dashboard-docker','harbor-local-gateway-docker','building-runner','aistrong'].includes(value))throw new Error('harbor_docker_runner_invalid');
  return value;
}
