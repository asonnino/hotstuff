import re
import sys

log_regexp = "^\[.*T(.*):(.*):(.*)\.(.*)Z DEBUG .*\] <APROF> (.*)$"
log_lines = re.findall(log_regexp, open(sys.argv[1]).read(), re.M)

print(f"Log lines {len(log_lines)}")

task_names = {}
task_parents = {}
task_wake = {}
task_states = {}


class TaskState:

    def __init__(self, no, time):
       self.no = no
       self.tot_time = 0
       self.blocked_time = 0
       self.pending_time = 0
       self.running_time = 0

       # inner
       self.prev_time = time
       self.can_work  = True
       self.running = False
       self.never_wake = True
       self.calls = 0

    def node_label(self):
        name = self.name.split(":")[0]
        if self.never_wake or self.calls == 0 or not self.to_print():
            return name

        per_call = (self.running_time + self.pending_time) / self.calls
        return f"{name} | {self.running_time}ms / {self.calls} | ({int(per_call*1000):}us P:{self.pending_time / (self.running_time + self.pending_time):.2%})"

    def to_print(self):
        return (self.running_time + self.pending_time > 10) or self.never_wake

    def summary(self):
        return f"R:{self.running_time:6} P:{self.pending_time:6} B:{self.blocked_time:6}    {self.name} "

    def name(self, name):
       self.name = name

    def resume(self, time):
        assert not self.running
        self.calls += 1
        period = time - self.prev_time
        if self.can_work:
            self.pending_time += period
        else:
            self.blocked_time += period
        self.tot_time += period

        # reset
        self.can_work = False
        self.running = True
        self.prev_time = time

    def pause(self, time):
        assert self.running
        period = time - self.prev_time
        self.running_time += period
        self.tot_time += period

        # reset
        self.running = False
        self.prev_time = time

    def signal(self, time):
        self.never_wake = False
        if self.can_work:
            return
        self.can_work = True
        if not self.running:
            period = time - self.prev_time
            self.blocked_time += period
            self.tot_time += period

            # reset
            self.prev_time = time


for (H,M,S,Mill,line) in log_lines:
    # print((H,M,S,Mill,line))
    time_ms = int(H)*60*60*1000 + int(M)*60*1000 + int(S)*1000 + int(Mill)

    # Task creation
    if line[:4] == "Task":
        # Define a task
        match_obj = re.match("Task (.*) from (.*) defined (.*)", line)
        task_no = match_obj.group(1)
        parent_no = match_obj.group(2)
        if parent_no == "None":
            parent_no = None
        else:
            # Strip the Some(*)
            parent_no = parent_no[5:-1]

        source = match_obj.group(3)

        task_names[task_no] = f"{source}-{task_no}"
        task_parents[task_no] = parent_no

        if task_no not in task_states:
            task_states[task_no] = TaskState(task_no, time_ms)
        task_states[task_no].name(task_names[task_no])

    # Wake relations
    if line[:4] == "Wake":
        match_obj = re.match("Wake: (.*) -> (.*)", line)
        source = match_obj.group(1)
        if source == "None":
            source = None
        else:
            source = source[5:-1]
        target = match_obj.group(2)

        pair = (source, target)
        task_states[target].signal(time_ms)
        if pair not in task_wake:
            task_wake[pair] = 1
        else:
            task_wake[pair] += 1

    if line[:4] == "Paus":
        task_no = line[len("Pause task: "):]
        task_states[task_no].pause(time_ms)

    if line[:4] == "Resu":
        task_no = line[len("Resume task: "):]
        if task_no not in task_states:
            task_states[task_no] = TaskState(task_no, time_ms)
        task_states[task_no].resume(time_ms)

wake_number = sum(task_wake.values())

show = {}

# Make a graph of task parent relations
parent_graph = open('parentgraph.dot', 'w')
print("digraph regexp {", file=parent_graph)
print('graph [ rankdir = "LR" ];', file=parent_graph)

for task_no in task_names:
    if task_states[task_no].to_print():
        print(f'{task_no} [label="{task_states[task_no].node_label()}", shape = "record"];', file=parent_graph)
        show[task_no] = True
    else:
        show[task_no] = False

for task_no in task_parents:
    if task_parents[task_no] is None:
        continue
    if task_states[task_no].to_print():
        if not show[task_parents[task_no]]:
            print(f'{task_parents[task_no]} [label="{task_states[task_parents[task_no]].node_label()}", shape = "record"];', file=parent_graph)
            show[task_parents[task_no]] = True

        print(f'{task_parents[task_no]} -> {task_no};', file=parent_graph)

print(f'edge [weight=1000 style=dashed color=dimgrey]', file=parent_graph)

for (source_no, target_no) in task_wake:
    pc = task_wake[(source_no, target_no)] / wake_number

    if source_no is None:
        source_no = "Env"

    if (source_no == "Env" or task_states[source_no].to_print()) and task_states[target_no].to_print():
        print(f'{source_no} -> {target_no} [label="{pc:.2%}"];', file=parent_graph)

print("}", file=parent_graph)

for task_no in task_states:
    print(task_states[task_no].summary())
