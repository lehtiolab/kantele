export const flashtime = 5000;

export const statecolors = {
  state: {
    wait: 'has-text-grey-light',
    pending: 'has-text-info',
    error: 'has-text-danger', 
    processing: 'has-text-warning', 
    revoking: 'has-text-grey-dark',
    done: 'has-text-success',
  },
  jobstate: {
    wait: 'has-text-grey-light',
    pending: 'has-text-info',
    queued: 'has-text-warning-light',
    error: 'has-text-danger', 
    processing: 'has-text-warning', 
    revoking: 'has-text-grey-dark',
    done: 'has-text-success',
    canceled: 'has-text-danger-dark',
  },
  // For datasets
  storestate: {
    cold: 'is-info',
    purged: 'is-danger', 
    complete: 'is-success', 
    'active-only': 'is-warning', 
    new: 'is-warning', 
    empty: 'is-light',
    broken: 'is-light',
  },
  smallstatus: {
    active: 'has-text-primary',
    deleted: 'has-text-grey',
    pending: 'has-text-danger',
  },
  // Analysis jobstate tags
  tag: {
    wait: 'is-grey-light',
    pending: 'is-info',
    error: 'is-danger', 
    processing: 'is-warning', 
    done: 'is-success',
  },
}


export const helptexts = {
  jobstate: {
    wait: 'WAITING',
    pending: 'PENDING',
    error: 'ERROR',
    processing: 'RUNNING',
    done: 'DONE',
  },
}
