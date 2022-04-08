import os
import time

from rich import pretty
from rich.console import Console

console = Console()
pretty.install()


topic_list = [
    #'academic-program-created',
    'academic-student-updated',
    'account-related',
    #'course-connection-faculty-updated',
    #'course-connection-status-updated',
    #'course-offering-connection-created',
    #'course-offering-date-updated',
    #'course-offering-primary-faculty-updated',
    'facility-created',
    'facility-updated',
    'ingress-created',
    'ingress-updated',
    'program-enrollment-created',
    'program-enrollment-updated',
    #'program-enrollment-course-opt-related',
    'service-created'
]


for topic in topic_list:
    try:
        console.print(
            f'[bold blue]Iniciando topico:[/bold blue] [italic green]{topic}[/italic green]'
        )
        stream = os.popen(
            f'confluent kafka topic consume {topic} -b --value-format avro > ~/Documents/trampo/topics-to-file-fv-athenas/{topic}_DEV.txt'
        )

        time.sleep(20)
        os.close(0)

        console.print(
            f'[bold red]Finalizando topico:[/bold red] [italic green]{topic}[/italic green]'
        )

        console.print(
            '\n===================================================================================\n'
        )
    except KeyboardInterrupt:
        console.print_exception('[bold red]Interrompido[/bold red]')
    except OSError as ose:
        pass

console.print('[bold purple]FINALIZADO[/bold purple]')