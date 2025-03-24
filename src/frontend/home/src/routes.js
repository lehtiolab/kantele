import Datasets from './Datasets.svelte'
import Jobs from './Jobs.svelte'
import Files from './Files.svelte'
import Analyses from './Analysis.svelte'
import Projects from './Projects.svelte'

const routes = {
    '/': Projects,
    '/datasets': Datasets,
    '/jobs': Jobs,
    '/projects': Projects,
    '/files': Files,
    '/analyses': Analyses,
}

export default routes;
