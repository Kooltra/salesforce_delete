from salesforce_bulk import CsvDictsAdapter
from salesforce_bulk import SalesforceBulk
from time import sleep
import sys
import math
import unicodecsv

salesforce_username = ''
salesforce_password = ''
salesforce_token = ''
object_type = 'EmailMessage'
query = "SELECT Id FROM {type}".format(type=object_type)
bulk = SalesforceBulk(
    username=salesforce_username,
    password=salesforce_password,
    security_token=salesforce_token)

def get_ids():
    print('creating bulk select job')
    job = bulk.create_query_job(object_type, contentType='CSV')
    batch = bulk.query(job,query)
    bulk.close_job(job)

    print('waiting for batch')
    while not bulk.is_batch_done(batch):
        print('.',end='')
        sys.stdout.flush()
        sleep(0.5)

    ids = []
    for result in bulk.get_all_results_for_query_batch(batch):
        reader = unicodecsv.DictReader(result, encoding='utf-8')
        for row in reader:
            ids.append(row['Id'])
    return ids


def del_ids(ids):
    print('creating bulk delete jobs')
    batchsize = 10000
    count = len(ids)
    batches = math.ceil(count/batchsize)
    remaining = count
    for batch in range(batches):
        print('batch %d of %d starting' % (batch+1, batches))
        batchsize = min(batchsize,remaining)
        batchstart = batch*batchsize

        job = bulk.create_delete_job(object_type, contentType='CSV')
        ids_dict = [dict(Id=idx) for idx in ids[batchstart:batchstart+batchsize]]
        print(ids_dict)
        csv_iter = CsvDictsAdapter(iter(ids_dict))
        batch = bulk.post_batch(job, csv_iter)
        bulk.close_job(job)

        print('waiting for batch')
        while not bulk.is_batch_done(batch):
            print('.',end='')
            sys.stdout.flush()
            sleep(0.5)

        for result in bulk.get_batch_results(batch):
            print(result)


def query_yes_no(question, default="no"):
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")
def main():
    ids = get_ids()
    if query_yes_no('delete {number} {type}s?'.format(type=object_type, number=len(ids))):
        del_ids(ids)

if __name__ == '__main__':
	main()
