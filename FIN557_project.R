
#Bankruptcy vs Covid case
setwd("C:/Users/Jason/Documents")
state_bank <- read.csv("state_bank.csv")
head(state_bank)
lm <- lm(bankruptcy_count_state~Covid_case, data=state_bank)
summary(lm)

library(tidyverse)
ggplot(data = state_bank) + 
  geom_point(mapping = aes(x=Covid_case, y=bankruptcy_count_state), color="blue") +
  geom_smooth(mapping = aes(x=Covid_case, y=bankruptcy_count_state), 
              color="orange", size=1, method="lm", se=F)+
  geom_text(aes(x = Covid_case, y = bankruptcy_count_state, label = State), 
            size = 3, color = "black", vjust = 1.5)
  


