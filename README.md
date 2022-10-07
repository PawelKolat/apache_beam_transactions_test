# Command to run with default value of 
# --input gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv --output output/results.jsonl.gz
python transactions.py  

# Example command to run against locally downloaded file to output/results-sum-from-local.jsonl.gz
python transactions.py --input 'Test/transactions_without.csv' --output results-sum-from-local

