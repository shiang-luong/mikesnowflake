"""this is a way to access bokeh palletes but for now it gives me a list of colors"""


from bokeh.palettes import Category20_20, Category20b_20, Category20c_20, PiYG11, Pastel1_9
COLORS = Category20_20 + Category20b_20 + Category20c_20 + PiYG11 + Pastel1_9


class ColorAccess(object):
    """this is color access"""

    def __init__(self):
        """init"""

        self.colors = Category20_20 + Category20b_20 + Category20c_20 + PiYG11 + Pastel1_9

    def getColors(self, N):
        """this will return a list of colors for a given length

        Args:
            N(int): the length of the list

        Returns:
            list of str: a list of hex colors from bokeh palletes

        Notes:
            colors are based on Category20, PiYG11 and Pastel1 palletes.
        """
        return self.colors[:N]
