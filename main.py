
from Proceso import *


def main():
    p = Proceso()

    print('***********************************')
    print('TIPO DE EJECUCION')
    print('\n 1. Lista las bases de datos y determina el peso incial')
    print('\n 2. Determina el peso final depues del mantenimiento')

    tipoInicio = input('Tipo de ejecucion: ')
    respuesta = ''

    print('***********************************')
    if tipoInicio == '1':
        print('\n Ejecuta 1.')
        respuesta = 'start'
    elif tipoInicio == '2':
        print('\n Ejecuta 2.')
        respuesta = 'end'
    else:
        print('No es una seleccion valida')
        print("Saliendo")
        exit()
    print('***********************************')

    p.mainProceso(respuesta)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Saliendo")
        exit()


