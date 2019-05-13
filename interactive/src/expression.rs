pub trait Expression {
    type Data;
    fn evaluate(&self, data: &[Self::Data]) -> Self::Data;
}