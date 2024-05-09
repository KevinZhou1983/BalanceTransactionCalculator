
--initial testing
delete from ConsumeTransactionDetail;
delete from  ConsumeTransaction;

update  chatHistory set chargeStatus=1 ;

update  imageHistory set chargeStatus=1 ;

update  customer set balance=10;

--
select * from chatHistory  where chargeStatus=1;

select * from imageHistory  where chargeStatus=1;

select * FROM ConsumeTransactionDetail where transactionStatus=1;

-- 
select * from ConsumeTransaction;
select * from customer ;

--
