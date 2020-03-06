from django.core.management.base import BaseCommand
from django.db.transaction import atomic

from core.models import RawData, WorkerSettings, AggData

BULK_ROW_COUNT = 1000


def aggregate_data(qs):
    """ Aggregates users events data.

    Returns:
        Following dict() object {'<UserId>': {'balance': <int>, 'max_amount': <int>, 'best_event_id': <EventId>}}
    """
    res = {}
    for row in qs.values():
        if not res.get(row['user_id']):
            res[row['user_id']] = {'balance': row['amount'], 'max_amount': row['amount'], 'best_event_id': row['event_id']}
            continue
        if res[row['user_id']]['max_amount'] < row['amount']:
            res[row['user_id']]['max_amount'] = row['amount']
            res[row['user_id']]['best_event_id'] = row['event_id']
        res[row['user_id']]['balance'] += row['amount']
    return res


def create_or_update_agg_data():
    """ Creates or updates aggregated users events data. """
    worker_settings = WorkerSettings.load()
    qs = RawData.objects.all()
    if worker_settings.last_checked_id >= qs.count():
        print("Please set last_checked_id to 0 in worker settings and re-run the command.")
        return
    qs = qs.filter(pk__range=(worker_settings.last_checked_id, worker_settings.last_checked_id + worker_settings.step))
    last_query_pk = qs.last().pk
    agg_data = aggregate_data(qs)
    user_ids = AggData.objects.values_list('user', flat=True)
    existing_agg_users = []
    new_agg_users = []
    for user_id in agg_data:
        if user_id in user_ids:
            obj = AggData.objects.get(user=user_id)
            obj.balance = agg_data[user_id]['balance']
            obj.best_event_id = agg_data[user_id]['best_event_id']
            existing_agg_users.append(obj)
        else:
            obj = AggData(user_id=user_id,
                          balance=agg_data[user_id]['balance'],
                          best_event_id=agg_data[user_id]['best_event_id'])
            new_agg_users.append(obj)
    with atomic():
        AggData.objects.bulk_update(existing_agg_users, ['balance', 'best_event'])
        AggData.objects.bulk_create(new_agg_users)
        worker_settings.last_checked_id = last_query_pk
        worker_settings.save()


class Command(BaseCommand):
    """ Django command to generate fake users and events. """

    def handle(self, *args, **options):
        self.stdout.write("Start aggrigating users and events.")
        create_or_update_agg_data()
        self.stdout.write(self.style.SUCCESS("Done!"))
