/* Generated By:JJTree: Do not edit this line. ASTGetLambdaFunction.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTGetLambdaFunction extends SimpleNode {
  public ASTGetLambdaFunction(int id) {
    super(id);
  }

  public ASTGetLambdaFunction(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=c03d613997ecf9fbb99535db83be806c (do not edit this line) */
