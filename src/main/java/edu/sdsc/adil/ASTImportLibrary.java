/* Generated By:JJTree: Do not edit this line. ASTImportLibrary.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTImportLibrary extends SimpleNode {
  public ASTImportLibrary(int id) {
    super(id);
  }

  public ASTImportLibrary(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=f8c32dd40daa00bd32110e677ad6842d (do not edit this line) */
