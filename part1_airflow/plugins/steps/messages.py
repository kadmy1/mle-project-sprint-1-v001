# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='7381583717:AAHxo4yLdDKJybe1Kke3WSDo6jEzRFqSvgQ',
                        chat_id='-4200913708')

    dag = context['dag']
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']

    message = f'Исполнение DAG {dag} с id={run_id} task={task_instance_key_str} прошло так себе!'
    hook.send_message({
        'chat_id': '-4200913708',
        'text': message
    }) 

def send_telegram_success_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='7381583717:AAHxo4yLdDKJybe1Kke3WSDo6jEzRFqSvgQ',
                        chat_id='-4200913708')

    dag = context['dag']
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']

    message = f'Исполнение DAG {dag} с id={run_id} task={task_instance_key_str} прошло успешно!'
    hook.send_message({
        'chat_id': '-4200913708',
        'text': message
    }) 
