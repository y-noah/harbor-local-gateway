// Both pools run Docker with services and real UID ownership support.
// The LUMI compute pool must never receive private-service or root-dependent jobs.
export function dockerRunnerTag(value='building-runner'){
  if(!['building-runner','aistrong'].includes(value))throw new Error('harbor_docker_runner_invalid');
  return value;
}
