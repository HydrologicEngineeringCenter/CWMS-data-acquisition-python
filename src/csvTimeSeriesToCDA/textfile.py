class TextFile:
    def __init__(self, filename=None, lines=None):
        if lines is not None:
            self.lines = lines
        elif filename is not None:
            with open(filename, 'r') as f:
                self.lines = [line.rstrip('\r\n') for line in f]
        else:
            raise ValueError("Either filename or lines must be provided")

    def find(self, entry):
        try:
            return self.lines.index(entry)
        except ValueError:
            return -1

    def subset(self, start, end):
        return self.lines[start:end]

    def subset_as_textfile(self, start, end):
        return TextFile(lines=self.lines[start:end+1])

    def find_beginning_with(self, text):
        for i, line in enumerate(self.lines):
            if line.startswith(text):
                return i
        return -1

    def get_value(self,text):
        """returns the value associated with text followed by an equal sign
        for example if the file looked like:
        line1=apples
        line2=frogs
        get_value(line1) would return 'apples'
         """
        prefix = text+"="
        idx = self.find_beginning_with(prefix)
        if idx >=0 :
            s = self.lines[idx]
            s = s.removeprefix(prefix)
            return s
        else:
            return ""

    def __len__(self):
        return len(self.lines)

    def find_all(self, entry):
        return [i for i, line in enumerate(self.lines) if line == entry]