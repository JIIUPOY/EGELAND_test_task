# Предоставленное тестовое задание

У вас есть доступ к базе данных Отдела продаж (PostgreSQL), куда загружаются данные об ответах менеджеров в сделках в 
amoCRM. Диалог с каждым клиентом ведётся внутри своей сделки. 

Необходимо: 

### Задание 1
Написать SQL-запрос, который будет рассчитывать среднее время ответа для каждого менеджера/пары менеджеров.  
Расчёт должен учитывать следующее:
<ul>
    <li>если в диалоге идут несколько сообщений подряд от клиента или менеджера, то при расчёте времени ответа надо 
учитывать только первое сообщение из каждого блока;</li>
    <li>менеджеры работают с 09:30 до 00:00, поэтому нерабочее время не должно учитываться в расчёте среднего времени 
ответа, т.е. если клиент написал в 23:59, а менеджер ответил в 09:30 – время ответа равно одной минуте;</li>
    <li>ответы на сообщения, пришедшие ночью также нужно учитывать.</li>
</ul>

### Задание 2
На основе базы данных из первого задания построить дашборд в DataLens с данными о среднем времени ответа менеджеров. 
Виды визуализаций и структура отчёта произвольные, однако необходима возможность фильтровать данные по дням, менеджерам 
и начальникам отделов продаж. 

### Задание 3
Решить первое задание при помощи Python и библиотеки pandas.

---------------
## Пояснения к SQL скрипту
1. Сначала необходимо отобрать только первые сообщения от клиента и менеджера в рамках одного диалога. Для этого будет 
использоваться оконная функция LAG с проверкой на то, что выбранный и предыдущий id диалога совпадают. В случае 
совпадения будет присвоена единица, иначе - 0 (is_first_message_in_block). Таким обзаром, будут отобраны первые 
сообщения.  
Этот подзапрос будет создаваться как вспомогательный запрос с использованием оператора WITH (message_groups).
2. В следующем вспомогательном запросе будут выбраны только те значение, для которых is_first_message_in_block равно единице. 
Также в задании сказано, что менеджеры могут отвечать ночью не в свое рабочее время. Поэтому также был добавлен еще CASE 
для того, чтобы все значения времени, которые меньше 9:30, то есть начала рабочего дня, округлялись до начала рабочего 
дня (9:30).  
Также создается с помощью WITH (working_hours_messages).
3. В третьем вспомогательном запросе будет рассчитываться время ответа каждого менеджера на сообщения клиентов. Для этого 
создается CASE, в которым условием проверки будет являться равенство или различие дат, то есть если день сообщения клиента 
и менеджера будет различаться, то будет браться время ответа менеджера (если оно меньше 9:30, то будет округляться до 
9:30) и вычитаться из времени начала рабочего дня менеджера и к этой разности будет прибавляться разность времен между 
временем 00:00 следующего дня от дня сообщения клиента и временем отправки сообщения клиента.  
После этого вся сумма будет делиться на 60, чтобы она хранилась в секундах. Данные манипуляции будут проводиться на данных 
из таблицы working_hours_messages, которая будет соеденена сама с собой - в первом случае это будут данные только о сообщениях 
клиентов, во втором случае - только от менеджеров.
4. В основном теле запроса будет выбрано id менеджера, его имя и применена агрегирующая функция (AVG) для рассчета среднего 
среднего времи ответа для каждого клиента. Данные будут отсортированы по возрастанию среднего времени ответа.
---
## Дополнительное пояснение
Также прикрепляю скрипт в формате .txt, чтобы в случае проблем с файлом .sql был запасной вторичный вариант.

---

## Пояснение о задании 2
В файле task_2 будет прикреплена ссылка на дашборд в Yandex DataLens.