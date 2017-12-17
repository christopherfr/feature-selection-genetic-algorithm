import curses
import ag
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def genetico(info_win, data_win):
    POBLACION = 20
    MAX_ITERACIONES = 100
    PORCENTAJE_MUTACION = 0.15

    conf = SparkConf().setAppName("Tarea3").setMaster("local")
    sc = SparkContext(conf=conf)
    ss = SparkSession(sc)

    f = open("log_ag_{}_{}_{}.txt".format(POBLACION,MAX_ITERACIONES,int(PORCENTAJE_MUTACION*100)),"a")

    poblacion = ag.Poblacion(POBLACION, ag.generador, ag.fitness, ag.f_reproduccion, ag.f_mutacion, PORCENTAJE_MUTACION, ss)
    data_win.addstr(0,0,"GENERACIONES:")
    for i in range(0,MAX_ITERACIONES):
        f.write("GENERACIÓN {}:\n".format(i+1))
        for j in range(0, len(poblacion.poblacion)):
            e = poblacion.poblacion[j]
            fitness = poblacion.fitness_results[j]
            data_win.addstr(j+1, 1, "{} ---> {}".format(e, fitness))
            f.write("{} ---> {}\n".format(e, fitness))
            data_win.refresh()

        f.write("\n")
        info_win.clear()
        info_win.addstr(0,1, "Numero de Generaciones:{}".format(i+1))
        info_win.addstr(1,1, "Promedio Fitness:{}".format(poblacion.promedio_fitness()))
        info_win.refresh()

        poblacion.seleccion()
        poblacion.reproduccion()
        #poblacion.mutar()

    #for j in range(0, len(poblacion.poblacion)):
    #    e = poblacion.poblacion[j]
    #    data_win.addstr(j+1, 1, "{} ---> {}".format(e, e.calcular_fitness()))
    #    data_win.refresh()
    #info_win.clear()
    #info_win.addstr(0,1, "Numero de Generaciones:{}".format(i+1))
    #info_win.addstr(1,1, "Promedio Fitness:{}".format(poblacion.promedio_fitness()))
    #info_win.refresh()

def main(stdscr):
    # Clear screen
    stdscr.clear()

    info_win = curses.newwin(curses.LINES,int(curses.COLS/5*2),0,0)

    data_win = curses.newwin(curses.LINES,int(curses.COLS/5*3),0,int(curses.COLS/5*2))

    data_win.border(0)
    info_win.refresh()
    genetico(info_win, data_win)
    #data_win.getkey()
    info_win.getkey()
    

curses.wrapper(main)
