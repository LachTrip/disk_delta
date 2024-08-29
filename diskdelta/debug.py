class Debug:
    isEnabled = False
    indentNum = 0

    @classmethod
    def enable(cls, debug=True):
        cls.isEnabled = debug

    @classmethod
    def log(cls, message, end="\n"):
        if not cls.isEnabled:
            return
        if end != "\n":
            print(message, end=end)
        else:
            print(cls.indentNum * "  " + message)

    @classmethod
    def increment_indent(cls, num=1):
        cls.indentNum += num