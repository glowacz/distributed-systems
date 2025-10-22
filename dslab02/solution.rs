use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

type Task = Box<dyn FnOnce() + Send>;

// You can define new types (e.g., structs) if you need.
// However, they shall not be public (i.e., do not use the `pub` keyword).

/// The thread pool.
pub struct Threadpool {
    // Add here any fields you need.
    // We suggest storing handles of the worker threads, submitted tasks,
    // and information whether the pool is running or is shutting down.

    active: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
    tasks_and_cvar: Arc<(Mutex<Vec<Task>>, Condvar)>
}

impl Threadpool {
    /// Create new thread pool with `workers_count` workers.
    pub fn new(workers_count: usize) -> Self {
        let active = Arc::new(AtomicBool::new(true));
        let mut handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(workers_count);
        let tasks_and_cvar = Arc::new((Mutex::new(vec![]), Condvar::new()));

        for _ in 0..workers_count {
            let active_clone = Arc::clone(&active);
            let sync_vars_clone = Arc::clone(&tasks_and_cvar);

            let handle = thread::spawn(move || {
                Threadpool::worker_loop(sync_vars_clone, active_clone);
            });
            handles.push(handle);
        }

        Threadpool {
            active,
            handles,
            tasks_and_cvar
        }
    }

    /// Submit a new task.
    pub fn submit(&self, task: Task) {
        let (mutex, cvar) = &*self.tasks_and_cvar;
        let mut tasks_mut = mutex.lock().unwrap();
        tasks_mut.push(task);
        cvar.notify_all();
    }

    fn worker_loop(tasks_and_cvar: Arc<(Mutex<Vec<Task>>, Condvar)>, active: Arc<AtomicBool>) {
        let (tasks_mut, cvar) = &*tasks_and_cvar;

        loop {
            let mut tasks_available = tasks_mut.lock().unwrap();

            if tasks_available.is_empty() {
                if !active.load(Ordering::Acquire) {
                    break;
                }

                tasks_available = cvar.wait(tasks_available).unwrap();
            }

            let task = tasks_available.pop();

            drop(tasks_available);

            match task {
                Some(task_fun) => task_fun(),
                None => println!("the task vector was empty")
            }
        }
    }
}

impl Drop for Threadpool {
    /// Gracefully end the thread pool.
    ///
    /// It waits until all submitted tasks are executed,
    /// and until all threads are joined.
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        self.tasks_and_cvar.1.notify_all();
        
        let handles = std::mem::take(&mut self.handles);

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
