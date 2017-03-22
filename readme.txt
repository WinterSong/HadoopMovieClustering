My project is implement on 3rd part python library, MRJob.
If you want to run it, first install it.
       pip install mrjob

To run it locally, you could run the following command:
       python project4.py /path/to/dataset_txts --mNum={number of dataset files} --jobconf mapred.reduce.tasks=1 [> output]

Or deploy it on hadoop:
        python project4.py -r hadoop /path/to/dataset_txts --mNum={number of dataset files} --jobconf mapred.reduce.tasks=1 [> output]
(hadoop start and configuration are omitted.)

