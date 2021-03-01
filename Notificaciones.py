from win10toast import ToastNotifier
import time

class Notificaciones:

    def notificar(self, titulo,mensaje):
        toaster = ToastNotifier()

        toaster.show_toast(
            titulo + "!", mensaje, threaded = True,
            icon_path=None, duration=6)

        while toaster.notification_active():
            time.sleep(0.1)
