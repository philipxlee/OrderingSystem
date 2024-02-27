from enum import Enum

"""
This class is used to define the order details and email addresses of the customers.
The OrderDetail enum contains the different types of items that can be purchased.
The OrderEmail enum contains the email addresses of the customers.
"""


class OrderDetail(Enum):
    SHOE = "shoe"
    SHIRT = "shirt"
    PANTS = "pants"
    HAT = "hat"
    SOCKS = "socks"
    GLOVES = "gloves"
    JACKET = "jacket"
    TIE = "tie"
    BELT = "belt"
    WATCH = "watch"


class OrderEmail(Enum):
    JOHN = "john@gmail.com"
    JANE = "jane@gmail.com"
    JIM = "jim@gmail.com"
    JILL = "jill@gmail.com"
    JACK = "jack@gmail.com"
    JEN = "jen@gmail.com"
    JERRY = "jenny@gmail.com"
    JEFF = "jeff@gmail.com"
    Amy = "amy@gmail.com"
    Andy = "andy@gmail.com"
