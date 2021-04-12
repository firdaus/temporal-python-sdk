# Steps to run this sample:

1. You need a Temporal service running. See details in README.md
2. Run the following command to start the worker

   ```bash
   python worker.py
   ```

3. Run the following command to start the example

   ```bash
   tctl workflow start --tq "hello-world" --wt "GreetingWorkflow::get_greeting" -et 10 -i '"World"'
   ```
