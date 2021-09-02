class lib_clase:
    """
    Instantiate a multiplication operation.
    """
    
    def __init__(self, date_hoy=None):
        self.date_hoy = date_hoy
    
    def multiplicador(numero, multiplo):
        """
        Multiply a given number by a given multiplier.
    
        :param number: The number to multiply.
        :type number: int
    
        :param muiltiplier: The multiplier.
        :type muiltiplier: int
        """
        
        return numero * multiplo
