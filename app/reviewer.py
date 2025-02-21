import gradio as gr
import tempfile
import os

from util.conf import get_conf
from util.review_paper import review_paper

conf = get_conf()


def review_tab():
    with gr.Tab("论文评审"):
        gr.Markdown("# Paper Review")
        
        with gr.Row():
            # 左侧列：上传PDF和进度
            with gr.Column(scale=1):
                pdf_input = gr.File(
                    label="上传PDF文件",
                    file_types=[".pdf"],
                    type="binary"
                )
                review_button = gr.Button("开始评审", variant="primary")
                # 添加进度显示
                progress_output = gr.Markdown("准备就绪", label="评审进度")
                # 添加token统计显示
                token_output = gr.Markdown("", label="Token统计")
            
            # 右侧列：显示评审结果
            with gr.Column(scale=2):
                review_output = gr.Markdown(
                    label="评审结果",
                    show_label=True,
                )
                copy_button = gr.Button("复制结果")

        def review_wrapper(file):
            if file is None:
                yield "请先上传PDF文件", "请先上传PDF文件", ""
                return
            
            progress_updates = []
            def update_progress(msg):
                progress_updates.append(msg)
                yield None, "\n".join(progress_updates), ""
            
            try:
                update_progress("1. 开始处理PDF文件...")
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_pdf:
                    temp_pdf.write(file)
                    temp_path = temp_pdf.name
                
                update_progress("2. PDF文件加载中...")
                
                # 创建生成器来处理评审过程
                for progress in review_paper(
                    temp_path,
                    progress_callback=update_progress
                ):
                    if isinstance(progress, tuple):
                        result, token_stats = progress
                        # 生成token统计markdown
                        token_md = f"""
### Token 使用统计
- 输入tokens: {token_stats['prompt_tokens']:,}
- 输出tokens: {token_stats['completion_tokens']:,}
- 总计tokens: {token_stats['total_tokens']:,}
- 预估费用: ¥{token_stats['total_cost']:.4f}

*定价: ¥0.05/千tokens*
"""
                        # 生成评审结果markdown
                        markdown_result = f"""
## 评审结果

**总体评分**: {result['Overall']}/10
**决定**: {result['Decision']}

### 优点
{chr(10).join([f'- {s}' for s in result['Strengths']])}

### 缺点
{chr(10).join([f'- {w}' for w in result['Weaknesses']])}

### 问题
{chr(10).join([f'- {q}' for q in result['Questions']])}

### 详细评分
- 原创性: {result['Originality']}/4
- 质量: {result['Quality']}/4
- 清晰度: {result['Clarity']}/4
- 重要性: {result['Significance']}/4
- 技术可靠性: {result['Soundness']}/4
- 展示: {result['Presentation']}/4
- 贡献: {result['Contribution']}/4
- 置信度: {result['Confidence']}/5
"""
                        update_progress("✅ 评审完成!")
                        yield markdown_result, "\n".join(progress_updates), token_md
                    
            except Exception as e:
                error_msg = f"❌ 评审过程出错: {str(e)}"
                update_progress(error_msg)
                yield "评审失败，请查看进度信息", "\n".join(progress_updates), ""
            finally:
                os.unlink(temp_path)

        review_button.click(
            fn=review_wrapper,
            inputs=[pdf_input],
            outputs=[review_output, progress_output, token_output],
            show_progress=True
        )

        # 添加复制功能
        copy_js = """
            (output) => {
                if (!output) return;
                navigator.clipboard.writeText(output);
                const notify = window.notifyOnSuccess || window.notify;
                if (notify) notify({ msg: "已复制到剪贴板！", type: "success" });
            }
        """
        copy_button.click(
            fn=None,
            inputs=[review_output],
            outputs=None,
            js=copy_js
        )
