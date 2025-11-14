import collections  # For the simple Queue (deque)
import heapq        # For the Priority Queue
import math         # For distance calculation

# --- I. FUNDAMENTAL CONCEPTS & ADTs ---
# Defining the Abstract Data Types (ADTs) for our system

class Job:
    """
    Represents a single job request.
    This is an ADT (Abstract Data Type) for a 'Job'.
    """
    def __init__(self, job_id, location, skill_required, priority):
        self.job_id = job_id
        self.location = location  # Tuple (x, y)
        self.skill_required = skill_required
        self.priority = priority  # 1 = High, 5 = Low
        self.status = "pending"
        self.assigned_worker = None
    
    def __str__(self):
        return f"[Job ID: {self.job_id}, Skill: {self.skill_required}, Prio: {self.priority}]"

class Worker:
    """
    Represents a single worker.
    This is an ADT (Abstract Data Type) for a 'Worker'.
    """
    def __init__(self, worker_id, name, location, skills):
        self.worker_id = worker_id
        self.name = name
        self.location = location  # Tuple (x, y)
        self.skills = skills      # List of strings, e.g., ["plumbing", "electrical"]
        self.is_available = True
    
    def __str__(self):
        return f"[Worker: {self.name}, Skills: {', '.join(self.skills)}]"

# --- II. CORE ALGORITHMS ---

def calculate_distance(loc1, loc2):
    """
    Helper function to calculate Euclidean distance between two (x, y) points.
    """
    return math.sqrt((loc1[0] - loc2[0])**2 + (loc1[1] - loc2[1])**2)

# --- III. SCHEDULER IMPLEMENTATION (NAIVE) ---

class SimpleScheduler:
    """
    A naive scheduler using:
    1. A simple Queue (FIFO) for jobs.
    2. A Linear Search (O(n)) to find the *first* available worker.
    """
    def __init__(self):
        # Using a list as a simple array for workers
        self.workers = []
        # Using a 'deque' as a simple FIFO Queue
        self.job_queue = collections.deque()
        
    def add_worker(self, worker):
        self.workers.append(worker)

    def add_job(self, job):
        self.job_queue.append(job) # Add to the back of the line
        print(f"New Job Added: {job.job_id} (Prio: {job.priority}). Queue size: {len(self.job_queue)}")

    def assign_next_job(self):
        if not self.job_queue:
            print("SIMPLE_SCHEDULER: No jobs in queue.")
            return

        # 1. Get job from front (FIFO)
        job_to_assign = self.job_queue.popleft() 
        print(f"\nSIMPLE_SCHEDULER: Attempting to assign Job {job_to_assign.job_id} (Prio: {job.priority})")

        # 2. Use Linear Search (O(n)) to find a worker
        found_worker = None
        for worker in self.workers:
            if (job_to_assign.skill_required in worker.skills and worker.is_available):
                found_worker = worker
                break # Found the first one, stop searching

        # 3. Assign if found
        if found_worker:
            job_to_assign.status = "assigned"
            job_to_assign.assigned_worker = found_worker.name
            found_worker.is_available = False
            print(f" -> SUCCESS: Assigned Job {job_to_assign.job_id} to {found_worker.name}")
        else:
            print(f" -> FAILED: No available worker for Job {job_to_assign.job_id}")
            # Put it back at the front, will cause a loop if no worker ever becomes available
            self.job_queue.appendleft(job_to_assign) 

    def complete_job(self, worker_name):
        # Helper to make a worker available again for demo purposes
        for worker in self.workers:
            if worker.name == worker_name:
                worker.is_available = True
                print(f"\nSIMPLE_SCHEDULER: {worker_name} is now available.")
                break

# --- IV. SCHEDULER IMPLEMENTATION (SMART) ---

class SmartScheduler:
    """
    A 'smart' scheduler using:
    1. A Priority Queue (Min-Heap) for jobs.
    2. A Sort-based (O(k log k)) search to find the *best* (closest) worker.
    """
    def __init__(self):
        self.workers = []
        # Use a 'heap' as a Priority Queue.
        self.job_priority_queue = [] # This is just a list, heapq functions work on it
        self.job_counter = 0 # Used as a tie-breaker for heap
        
    def add_worker(self, worker):
        self.workers.append(worker)

    def add_job(self, job):
        # We push a tuple: (priority, tie_breaker, job_object)
        # The tie_breaker ensures if priorities are equal, it orders by insertion.
        heapq.heappush(self.job_priority_queue, (job.priority, self.job_counter, job))
        self.job_counter += 1
        print(f"New Job Added: {job.job_id} (Prio: {job.priority}). Queue size: {len(self.job_priority_queue)}")

    def assign_next_job(self):
        if not self.job_priority_queue:
            print("SMART_SCHEDULER: No jobs in queue.")
            return

        # 1. Pop the job with the *lowest* priority number (highest urgency)
        # This is an O(log n) operation
        priority, _, job_to_assign = heapq.heappop(self.job_priority_queue)
        print(f"\nSMART_SCHEDULER: Attempting to assign Job {job_to_assign.job_id} (Prio: {priority})")

        # 2. Find ALL available, skilled workers (O(n))
        candidate_workers = []
        for worker in self.workers:
            if (job_to_assign.skill_required in worker.skills and worker.is_available):
                candidate_workers.append(worker)

        if not candidate_workers:
            print(f" -> FAILED: No available worker for Job {job_to_assign.job_id}")
            # Put it back in the queue
            self.add_job(job_to_assign) 
            return

        # 3. Find the BEST worker using Sorting (O(k log k))
        # Calculate a "cost" (distance) for each candidate
        worker_with_cost = []
        for worker in candidate_workers:
            distance = calculate_distance(job_to_assign.location, worker.location)
            worker_with_cost.append((distance, worker)) # Tuple: (cost, worker_object)

        # Sort the candidates by cost (distance)
        worker_with_cost.sort(key=lambda x: x[0]) 

        # 4. Assign to the BEST worker (index 0)
        best_distance, best_worker = worker_with_cost[0]
        
        best_worker.is_available = False
        job_to_assign.status = "assigned"
        job_to_assign.assigned_worker = best_worker.name
        
        print(f" -> SUCCESS: Assigned Job {job_to_assign.job_id} to {best_worker.name}")
        print(f"    -> Details: {len(candidate_workers)} candidates. Chose closest at {best_distance:.2f} units.")


    def complete_job(self, worker_name):
        # Helper to make a worker available again for demo purposes
        for worker in self.workers:
            if worker.name == worker_name:
                worker.is_available = True
                print(f"\nSMART_SCHEDULER: {worker.name} is now available.")
                break

# --- V. DEMONSTRATION ---

if __name__ == "__main__":
    
    # 1. Create Sample Data
    
    # --- Workers ---
    workers_list = [
        Worker("w1", "Alice", (10, 10), ["plumbing", "electrical"]),
        Worker("w2", "Bob", (1, 1), ["plumbing", "carpentry"]),
        Worker("w3", "Charlie", (5, 5), ["electrical", "painting"]),
    ]
    
    # --- Jobs ---
    # Job 4 (Prio 1) is added *last* but should be assigned *first* by SmartScheduler
    jobs_list = [
        Job("j1", (9, 9), "plumbing", 3),    # Close to Alice
        Job("j2", (6, 6), "electrical", 2),  # Close to Charlie
        Job("j3", (1, 2), "carpentry", 3),   # Only Bob can do
        Job("j4", (5, 6), "electrical", 1),  # HIGH PRIORITY
    ]
    
    # --- 2. Run the "SmartScheduler" Demo ---
    print("===============================")
    print("RUNNING SMART SCHEDULER")
    print("===============================")
    
    smart_scheduler = SmartScheduler()

    # Add workers
    for w in workers_list:
        smart_scheduler.add_worker(w)
        
    # Add jobs (note the order)
    smart_scheduler.add_job(jobs_list[0]) # j1 (Prio 3)
    smart_scheduler.add_job(jobs_list[1]) # j2 (Prio 2)
    smart_scheduler.add_job(jobs_list[2]) # j3 (Prio 3)
    smart_scheduler.add_job(jobs_list[3]) # j4 (Prio 1) - Added last!

    print("\n--- Assigning all jobs... ---")
    
    # Assign all jobs from the queue
    # The Priority Queue will correctly pop j4, then j2, then j1, then j3
    smart_scheduler.assign_next_job() # Should assign j4 (Prio 1) to Charlie
    smart_scheduler.assign_next_job() # Should assign j2 (Prio 2) to Alice (Charlie is busy)
    smart_scheduler.assign_next_job() # Should assign j1 (Prio 3) to Bob (Alice is busy)
    smart_scheduler.assign_next_job() # Should assign j3 (Prio 3) - FAILED (Bob is busy)
    
    # Let's say Bob finishes his job
    smart_scheduler.complete_job("Bob")
    smart_scheduler.assign_next_job() # Should now assign j3 (Prio 3) to Bob


    # --- 3. Run the "SimpleScheduler" Demo for Comparison ---
    print("\n\n===============================")
    print(" RUNNING SIMPLE SCHEDULER")
    print("===============================")

    # Reset worker availability for the new demo
    workers_list_simple = [
        Worker("w1", "Alice", (10, 10), ["plumbing", "electrical"]),
        Worker("w2", "Bob", (1, 1), ["plumbing", "carpentry"]),
        Worker("w3", "Charlie", (5, 5), ["electrical", "painting"]),
    ]
    
    simple_scheduler = SimpleScheduler()

    for w in workers_list_simple:
        simple_scheduler.add_worker(w)

    # Add jobs in the same order
    simple_scheduler.add_job(jobs_list[0]) # j1 (Prio 3)
    simple_scheduler.add_job(jobs_list[1]) # j2 (Prio 2)
    simple_scheduler.add_job(jobs_list[2]) # j3 (Prio 3)
    simple_scheduler.add_job(jobs_list[3]) # j4 (Prio 1) - Added last!