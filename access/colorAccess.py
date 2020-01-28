"""this is a way to access bokeh palletes but for now it gives me a list of colors"""


from bokeh.palettes import Category20_20, Category20b_20, Category20c_20, PiYG11, Pastel1_9, BuPu9, Spectral4
COLORS = Category20_20 + Category20b_20 + Category20c_20 + PiYG11 + Pastel1_9 + BuPu9  # a list of 89 colors


class ColorAccess(object):
    """this is color access"""

    def __init__(self):
        """init"""

        self.colors = COLORS

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

    def getNodeDefaultColor(self):
        """this will grab the default color for directed graph nodes

        Returns:
            str: the default hex color
        """
        # blue
        return Category20_20[1]

    def getNodeHoverColor(self):
        """this will grab the hover color for directed graph nodes

        Returns:
            str: the default hex color
        """
        # avocado green
        return Spectral4[1]

    def getNodeSelectColor(self):
        """this will grab the hover color for directed graph nodes

        Returns:
            str: the default hex color
        """
        # orange
        return Spectral4[2]