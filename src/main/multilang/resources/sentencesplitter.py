import storm

class SentenceSplitterBolt(storm.BasicBolt):
    def process(self, tup):
        words = tup.values[0].split(" ")
        for word in words:
          storm.emit(["yh-mine:"+word])

SentenceSplitterBolt().run()