import random
flag=True
while flag:
  answer=random.randint(0,100)
  print(answer)
  counter = 1
  while counter<=6:
    while True:
      j=input('Guess the lucky number between 0 and 100: ')
      try:
        number=eval(j)
      except:
        print('Invalid input...try to guess a number')
      else:
        break    
    if counter==5:
      print('Game Over! The correct answer was', answer)  
      break
    elif number==answer:
      print('Good guess!', answer, " was the lucky number ;-)")
      break  
    elif number >= answer:
      print('Try to guess a lower number...you have ', 5-counter, ' guesses')
      counter +=1;
    else:
      print('Try to guess a higher number...you have ', 5-counter, ' guesses')
      counter +=1;
  x=input('Would you like to quit? (Y/N): ')
  if x=='N':
    flag=True
  else:
    flag=False         

